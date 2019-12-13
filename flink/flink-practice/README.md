# Flink-practice

## Introduction📖
> Some practices to learn Flink<p>

>Including
>- [TopN](#TopN)
>-

## TopN
> TopN 统计是常见场景，比如做实时统计排行榜，大屏展示等。<p>
> 两种场景, 全局TopN 和 分组 TopN, 两种结合起来还有一种嵌套 TopN 的场景，其主要也是为了解决全局 TopN 的数据热点问题<p>
> 全局topN的缺陷是，所有的数据只能汇集到一个节点进行 TopN 的计算，那么计算能力就会受限于单台机器，容易产生数据热点问题。
解决思路就是使用嵌套 TopN，或者说两层 TopN。在原先的 TopN 前面，再加一层 TopN，用于分散热点。例如可以先加一层分组 TopN，第一层会计算出每一组的 TopN，而后在第二层中进行合并汇总，得到最终的全网TopN。第二层虽然仍是单点，但是大量的计算量由第一层分担了，而第一层是可以水平扩展的
> <p>ps: 这也是解决数据倾斜的一种思路