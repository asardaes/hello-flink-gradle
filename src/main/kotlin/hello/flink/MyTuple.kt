package hello.flink

import java.sql.Timestamp

data class MyTuple(
        var ts: Timestamp = Timestamp(0),
        var a: String = "",
        var b: String = ""
)
