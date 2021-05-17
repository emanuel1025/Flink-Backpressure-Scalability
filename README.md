# Flink Backpressure Scalability Test
Basic Implementation of Flink - Batch &amp; Streaming

Windows are at the heart of processing infinite streams. Windows split the stream into “buckets” of finite size, over which we can apply computations. This document focuses on how windowing is performed in Flink and how the programmer can benefit to the maximum from its offered functionality.

Tested a stream of 25 million objects on a sliding streaming window of size ten seconds based on timestamp. Achieved a velocity of 2.5 million events/per second processed each for each window batch of ten seconds.
