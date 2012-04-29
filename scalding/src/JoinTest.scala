package org.hackreduce.team7.icgc

import com.twitter.scalding._

class JoinTestJob(args : Args) extends Job(args) {
  val job2 = Tsv(args("input2"), ('key2, 'tail2))
  Tsv(args("input1"), ('key1, 'tail1))
    .joinWithSmaller('key1 -> 'key2, job2)
    .write(Tsv(args("output")))
}
