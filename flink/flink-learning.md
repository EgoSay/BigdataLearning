# Flink 学习
[官方链接](https://flink.apache.org/zh/)

## Flink-basis
>Flink基础学习
### Flink Window
>参考链接: [Flink Window基本概念与实现原理](https://juejin.im/post/5cb6d4426fb9a068af37aa60)

Flink 有 3 个内置 Window
- 以事件数量驱动的Count Window
    >计数窗口，采用事件数量作为窗口处理依据, 分为滚动和滑动窗口两类
- 以会话间隔驱动的Session Window
    >会话窗口，采用会话持续时长作为窗口处理依据。设置指定的会话持续时长时间，
    在这段时间中不再出现会话则认为超出会话时长
- 以时间驱动的Time Window
    >时间窗口，采用时间作为窗口处理依据, 分为滚动和滑动窗口两类(感觉和`CountWindow`类型)

## 利用 `K8s` 部署`Flink`
参考链接： [使用 Kubernetes 部署 Flink 应用](http://shzhangji.com/cnblogs/2019/08/25/deploy-flink-job-cluster-on-kubernetes/)