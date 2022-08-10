
# forked from antoninbas/p4runtime-go-client

# 添加部分功能
1. client 增加一个 WriteManyUpdate 方法[client.go](pkg/client/client.go)
2. 计数器增加一个 ModifyManyCounterEntry 方法 [counters.go](pkg/client/counters.go)
3. multicast_group_entry 增加 ModifyMulticastGroup 方法,增加读取的方法ReadMulticastGroupWildcard和ReadMulticastGroup [pre.go](pkg/client/pre.go)
4. 增加clone_session_entry 的 insert/modify/delete 方法和read方法 [pre_clone.go](pkg/client/pre_clone.go)

# p4runtime-go-client
Go client for P4Runtime

**This project is still a work in progress. We make no guarantee for the
  stability of the API and may modify existing functions in the client API. We
  welcome feedback and contributions!.**

For a good place to start, check out the [l2_switch
example](cmd/l2_switch/README.md).
