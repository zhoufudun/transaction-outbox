# transaction-outbox 框架学习文档

> 基于当前仓库版本 `7.1.785`（Java 17）整理。本文关注该项目如何实现 Transactional Outbox，以及阅读源码和在 Spring 应用中落地的路径。

## 1. 项目解决的问题

一个服务在处理业务请求时通常同时做两件事：

1. 修改本地数据库，例如创建订单；
2. 通知外部系统，例如发送 Kafka 消息、调用库存服务或投递邮件。

若把这两件事直接顺序执行，就会有一致性缺口：数据库提交失败但消息已经发送，或数据库成功但外部调用失败。`transaction-outbox` 的做法是：**不在业务事务中直接做外部调用，而是将“稍后要执行的方法调用”写入同一个数据库事务。**

```text
业务事务
├─ INSERT / UPDATE 业务数据
└─ INSERT TXNO_OUTBOX（序列化后的目标方法调用）
       │
       ├─ 事务回滚：两者都不存在
       └─ 事务提交：异步执行；失败后由后台任务持续补偿
```

因此它保证的是“业务数据与待处理任务记录的原子性”；外部调用仍然是异步、最终一致，并且需要能承受重复投递。

## 2. 项目框架

### 2.1 模块划分

| 模块 | 职责 |
|---|---|
| `transactionoutbox-core` | 框架内核：代理、事务抽象、持久化、重试、顺序消费、序列化 SPI。 |
| `transactionoutbox-spring` | 对接 Spring 事务同步机制和 Spring 容器取 Bean。 |
| `transactionoutbox-guice` | 对接 Guice 注入。 |
| `transactionoutbox-jooq` | 对接 jOOQ 的线程绑定或显式传递事务上下文。 |
| `transactionoutbox-quarkus` | 对接 CDI / Quarkus 事务。 |
| `transactionoutbox-jackson` | 处理更复杂的多态 JSON 序列化。 |
| `transactionoutbox-testing` | 测试基类、监听器和测试辅助组件。 |
| `transactionoutbox-acceptance` | 基于 Testcontainers 的多数据库验收测试。 |

### 2.2 核心抽象

```text
TransactionOutbox                       门面：调度、flush、解除阻塞
        │
        └── TransactionOutboxImpl        主状态机
              ├── TransactionManager     获取当前事务 / 新建执行事务
              ├── Persistor              写入、查询、锁定 outbox 表
              ├── Instantiator           根据类名从 DI 容器创建目标对象
              ├── Submitter              投递本地线程池或远程节点
              ├── InvocationSerializer   序列化 / 反序列化方法调用
              └── TransactionOutboxListener  可观测性、告警、上下文传播
```

这种设计将“业务框架差异”隔离在 SPI 外：Spring/jOOQ 的区别主要落在 `TransactionManager` 和 `Instantiator`，核心调度和重试逻辑不需要改动。

## 3. 运行原理

### 3.1 调度阶段：代理调用变成数据库记录

业务代码看起来像普通方法调用：

```java
@Transactional
public void createOrder(CreateOrderCommand command) {
  orderRepository.save(Order.create(command));
  outbox.schedule(OrderEventPublisher.class)
      .orderCreated(command.orderId());
}
```

但 `schedule(OrderEventPublisher.class)` 返回的是动态代理。调用 `orderCreated` 时，框架不会执行真实方法，而是：

1. 从 `TransactionManager` 取得当前业务事务；
2. 创建 `Invocation`（目标类名、方法名、参数类型、参数、MDC/session）；
3. 创建 `TransactionOutboxEntry` 并通过 `Persistor` 写入 `TXNO_OUTBOX`；
4. 注册 post-commit hook；只有事务真正提交后才异步投递任务。

调用路径：

```text
TransactionOutbox.schedule
  → ProxyFactory.createProxy
  → TransactionOutboxImpl.schedule（代理拦截）
  → TransactionManager.extractTransaction
  → DefaultPersistor.save
  → Transaction.addPostCommitHook
```

### 3.2 执行阶段：在新事务中锁定、恢复、调用

提交后，默认 `Submitter` 将任务放入有界线程池。无论是立即投递还是后台 `flush()` 补偿，最终都会进入 `processNow`：

```text
processNow
  → 开启新事务
  → SELECT ... FOR UPDATE（并校验 version）
  → 从数据库重新反序列化 Invocation
  → Instantiator.getInstance(...) 获取真实 Bean
  → 反射调用目标方法
  → 成功：删除记录，或标为 processed 并保留去重窗口
  → 失败：attempts + 1，写入下一次允许尝试的时间
```

再次从数据库反序列化而不是直接使用内存对象很重要：它保证“首次异步执行”和“进程重启后的重试执行”走相同的数据路径，能尽早暴露序列化配置问题。

### 3.3 后台补偿与失败状态

后台任务应该周期性调用 `outbox.flush()`。它并行完成三类工作：

| 工作 | 含义 |
|---|---|
| 普通任务扫描 | 找出 `nextAttemptTime` 已到、未阻塞、未完成的无序任务。 |
| topic 扫描 | 每个 topic 仅取最小序号的未完成任务，以保证 topic 内 FIFO。 |
| 过期清理 | 清理已成功、但为 `uniqueRequestId` 去重而临时保留的记录。 |

普通任务达到 `blockAfterAttempts`（默认 5）后会置为 `blocked=true`，需要告警并人工/自动调用 `unblock(entryId)` 重新激活。topic 中的任务不会按此阈值丢入死信；失败的头部任务会阻塞该 topic，避免破坏顺序。

## 4. 表与一致性语义

默认 `DefaultPersistor` 使用：

| 表 / 字段 | 用途 |
|---|---|
| `TXNO_OUTBOX` | 主任务表，保存调用内容与状态。 |
| `invocation` | 序列化后的 `Invocation` JSON。 |
| `nextAttemptTime` / `lastAttemptTime` | 调度与退避时间。 |
| `attempts` / `blocked` | 重试次数与死信状态。 |
| `processed` | 已成功但因幂等保护尚未删除。 |
| `version` | 乐观锁版本，防止多个实例同时更新同一行。 |
| `uniqueRequestId` | 全局唯一的请求键；数据库唯一约束负责拒绝重复调度。 |
| `topic` / `seq` 与 `TXNO_SEQUENCE` | topic 内严格顺序的序号分配。 |
| `TXNO_VERSION` | 内置数据库迁移版本。 |

框架提供的是 **at-least-once（至少一次）** 处理语义：假设外部调用成功后，应用在删除 outbox 行之前崩溃，任务会被再次调用。因此目标方法及其下游必须幂等。

`uniqueRequestId` 只解决“同一入站请求被重复调度”的问题，例如消息队列重复投递；它不替代外部系统的幂等键、去重表或业务状态机。

## 5. 关键代码注释

以下代码为便于学习而简化，逻辑对应源码；省略日志、异常包装和部分参数校验。

### 5.1 代理拦截：把方法调用存入当前事务

源码：[TransactionOutboxImpl.java](../transactionoutbox-core/src/main/java/com/gruelbox/transactionoutbox/TransactionOutboxImpl.java)

```java
private <T> T schedule(Class<T> clazz, String uniqueRequestId,
                        String topic, Duration delayForAtLeast) {
  return proxyFactory.createProxy(
      clazz,
      (method, args) -> {
        // 1) 对 Spring 来说这里取线程绑定事务；对 jOOQ 也可从方法参数提取上下文。
        var extracted = transactionManager.extractTransaction(method, args);

        // 2) 保存的是“未来如何重放”的描述，而不是直接保存一个事件对象。
        TransactionOutboxEntry entry = newEntry(
            extracted.getClazz(), extracted.getMethodName(),
            extracted.getParameters(), extracted.getArgs(),
            uniqueRequestId, topic);

        // 3) 使用与业务数据相同的事务连接写 outbox 行。
        persistor.save(extracted.getTransaction(), entry);

        // 4) afterCommit 才允许投递，回滚时此 hook 不会运行。
        extracted.getTransaction().addPostCommitHook(() -> submitNow(entry));
        return null; // 代理调用没有同步结果；被调度的方法应使用 void 返回值。
      });
}
```

关键点：`Persistor.save` 不能自行开启另一个事务，否则 outbox 行可能在业务事务失败时残留，模式的基本保证就被破坏。

### 5.2 持久化：普通任务批量写入，幂等任务立即校验唯一约束

源码：[DefaultPersistor.java](../transactionoutbox-core/src/main/java/com/gruelbox/transactionoutbox/DefaultPersistor.java)

```java
public void save(Transaction tx, TransactionOutboxEntry entry) throws SQLException {
  // invocation 会先被序列化为 JSON，写入同一 JDBC transaction。
  var writer = new StringWriter();
  serializer.serializeInvocation(entry.getInvocation(), writer);

  // topic 任务在写入前分配递增序号，供之后按 topic FIFO 拉取。
  if (entry.getTopic() != null) {
    setNextSequence(tx, entry);
  }

  PreparedStatement stmt = tx.prepareBatchStatement(INSERT_SQL);
  setupInsert(entry, writer, stmt);

  if (entry.getUniqueRequestId() == null) {
    stmt.addBatch(); // 普通任务：在事务提交前批量刷出。
  } else {
    try {
      stmt.executeUpdate(); // 幂等任务：立即触发唯一索引冲突检查。
    } catch (Exception e) {
      if (indexViolation(e)) {
        throw new AlreadyScheduledException(...);
      }
      throw e;
    }
  }
}
```

关键点：立即 `executeUpdate()` 并不是脱离事务提交，而是为了在当前调用内及时把重复键转换成 `AlreadyScheduledException`。

### 5.3 执行与删除：锁和外部副作用在同一个“执行事务”中

源码：[TransactionOutboxImpl.java](../transactionoutbox-core/src/main/java/com/gruelbox/transactionoutbox/TransactionOutboxImpl.java)

```java
success = transactionManager.inTransactionReturnsThrows(tx -> {
  // SELECT ... FOR UPDATE / READPAST，并带 version 条件。
  if (!persistor.lock(tx, entry)) {
    return false; // 已被其他节点抢到、已处理或锁超时。
  }

  // 从 DI 容器或反射创建真实服务对象，再反射调用目标方法。
  invoke(entry, tx);

  if (entry.getUniqueRequestId() == null) {
    persistor.delete(tx, entry); // 成功后删除普通任务。
  } else {
    // 成功任务保留一段时间，以拒绝同 uniqueRequestId 的重复请求。
    entry.setProcessed(true);
    entry.setNextAttemptTime(after(retentionThreshold));
    persistor.update(tx, entry);
  }
  return true;
});
```

关键点：若 `invoke` 抛异常，整个执行事务会回滚，随后框架在新事务内增加 `attempts`、设置下次尝试时间。无法做到“外部系统已成功但本地删除失败”的原子提交，因此外部副作用必须幂等。

### 5.4 失败处理：指数退避需要自行扩展

源码目前使用固定的 `attemptFrequency` 作为下一次可执行时间：

```java
entry.setAttempts(entry.getAttempts() + 1);
entry.setBlocked(entry.getAttempts() >= blockAfterAttempts);
entry.setLastAttemptTime(clock.instant());
entry.setNextAttemptTime(clock.instant().plus(attemptFrequency));
persistor.update(tx, entry);
```

这意味着默认实现不是指数退避。若下游故障时需要更平滑的流量恢复，可通过替换/装饰 `Persistor` 或扩展处理策略实现 `base * 2^attempts + jitter`，并避免所有失败请求同时重试。

## 6. Spring 集成说明

`SpringTransactionManager` 的两个职责：

1. 用 `TransactionSynchronizationManager` 判断和复用当前 `@Transactional` 事务；
2. 注册 `afterCommit` 回调，确保事务提交后才投递任务。

`SpringInstantiator` 则在执行任务时通过 Spring 容器获取真正的 Bean。因此可以调度 Spring Bean，且可使用 `outbox.schedule(getClass())` 进行自调用；实际重放时获得的仍是容器管理对象。

简化配置：

```java
@Bean
TransactionOutbox transactionOutbox(
    SpringTransactionManager transactionManager,
    SpringInstantiator instantiator) {
  return TransactionOutbox.builder()
      .transactionManager(transactionManager)
      .instantiator(instantiator)
      .persistor(Persistor.forDialect(Dialect.POSTGRESQL_9))
      .listener(outboxListener())
      .build();
}
```

后台轮询可参考 [TransactionOutboxBackgroundProcessor.java](../transactionoutbox-spring/src/test/java/com/gruelbox/transactionoutbox/spring/example/simple/TransactionOutboxBackgroundProcessor.java)。任务使用 topic 时不会在提交后立即执行，必须依赖 `flush()`。

## 7. 建议的学习步骤

### 第一步：理解问题边界

阅读根目录 `README.md` 的 “Attempt 1 / Attempt 2 / Attempt 3”。能清楚回答以下问题即为达标：

- 为什么 `@Transactional` 不等于外部消息也在同一事务中？
- 为什么 Outbox 后仍然需要幂等？
- 为什么“把消息写进本地表”比“同步发消息”更可靠？

### 第二步：跟踪一次普通任务

按顺序阅读：

1. `TransactionOutbox.schedule`；
2. `TransactionOutboxImpl.schedule`；
3. `DefaultPersistor.save`；
4. `TransactionOutboxImpl.processNowInternal`；
5. `DefaultPersistor.lock / delete / update`。

建议用断点观察一次任务的 `TXNO_OUTBOX` 行从插入、锁定到删除的过程。

### 第三步：理解事务适配

阅读 `TransactionManager`、`Transaction` 与 `SpringTransactionManager`，重点区分：

- **调度时事务**：与业务数据共用，负责保存 outbox 行；
- **执行时事务**：新的事务，负责锁定 outbox 行和执行任务内的本地数据库操作。

### 第四步：学习可靠性机制

分别做四个小实验：

1. 业务方法抛异常，确认 outbox 行不落库；
2. 目标方法第一次抛异常、第二次成功，确认 `flush()` 重试；
3. 同一 `uniqueRequestId` 调度两次，确认第二次抛 `AlreadyScheduledException`；
4. 同一 topic 内第一条失败，确认后一条不会越过它执行。

### 第五步：学习生产扩展

最后阅读 `Submitter`、`TransactionOutboxListener`、`InvocationSerializer`：

- 用 Listener 发 blocked 告警、埋点，并传递 trace context；
- 用自定义 Submitter 将执行请求分发到集群；
- 用自定义 Serializer 支持复杂对象或加密敏感参数；
- 用自定义 Persistor / Dialect 对接现有表结构或数据库方言。

## 8. 落地检查清单

- [ ] 业务数据和 `TXNO_OUTBOX` 使用相同的数据源与事务管理器。
- [ ] 每个下游操作都具备幂等键、去重逻辑或可重放的状态机。
- [ ] 周期性执行 `flush()`，并在每个实例或明确选定的 worker 上运行。
- [ ] 监听 `blocked` 事件并建立告警、排障和 `unblock` 流程。
- [ ] 使用有界线程池；线程池饱和时由数据库积压承担背压。
- [ ] 根据数据库选择正确 `Dialect`；不支持 `SKIP LOCKED` 的数据库避免过多并发 flush。
- [ ] 评审所有方法参数的序列化兼容性，避免把 Entity、连接、懒加载代理等运行时对象写入 outbox。
- [ ] 敏感参数、MDC、session 采用加密序列化或不写入 outbox。
- [ ] 用 Flyway/Liquibase 管理表时关闭内置迁移，并将框架提供的 schema 纳入正式迁移。

## 9. 一句话总结

`transaction-outbox` 的本质是：**把不可原子提交的外部调用，转换成数据库事务中可持久化、可锁定、可重试的方法调用任务；以“至少一次 + 下游幂等”换取可靠的最终一致性。**
