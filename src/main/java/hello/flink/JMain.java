package hello.flink;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.Slide;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

public class JMain {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment execEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(execEnv);

        tableEnv.registerFunction("enlist", new Enlister());

        DataSource<MyTuple> source = execEnv.fromElements(
                new MyTuple(Timestamp.valueOf("2018-09-20 22:00:00"), "a", "1"),
                new MyTuple(Timestamp.valueOf("2018-09-20 22:01:00"), "a", "2"),
                new MyTuple(Timestamp.valueOf("2018-09-20 22:02:00"), "a", "3"),
                new MyTuple(Timestamp.valueOf("2018-09-20 22:00:00"), "b", "1"),
                new MyTuple(Timestamp.valueOf("2018-09-20 22:01:00"), "b", "2")
        );

        Table table = tableEnv.fromDataSet(source)
                .window(Slide.over("2.minutes").every("1.minute").on("ts").as("w"))
                .groupBy("a, w")
                .select("a, w.start as w_start, enlist(a, b)")
                .orderBy("a, w_start ASC");

        String[] fieldNames = new String[] {
                "a",
                "w_start",
                "result"
        };
        TypeInformation[] types = new TypeInformation[] {
                Types.STRING(),
                Types.SQL_TIMESTAMP(),
                TypeInformation.of(new TypeHint<IResult<String>>(){})
        };
        TypeInformation<Row> typeInfo = Types.ROW(fieldNames, types);
        tableEnv.toDataSet(table, typeInfo).print();

        System.out.println();
        tableEnv.toDataSet(table, Row.class).print();
    }

    public interface Foo<T extends Serializable> extends Serializable {
        T sayHi();
    }

    public interface IResult<T extends  Serializable> extends Serializable {
        @SuppressWarnings("unused")
        Collection<Foo<T>> get();
    }

    public static class Result<T extends Serializable> implements IResult<T> {
        private final List<Foo<T>> result;
        Result(Collection<Foo<T>> result) { this.result = new ArrayList<>(result); }
        @Override
        public Collection<Foo<T>> get() { return result; }
        @Override
        public String toString() {
            return String.join("->",
                    result.stream().
                            map(foo -> foo.sayHi().toString())
                            .collect(Collectors.toList()));
        }
    }

    public static class Container<T extends Serializable> implements Serializable {
        private final Map<Integer, Foo<T>> stuff = new HashMap<>();
        void add(Foo<T> e) { stuff.put(e.hashCode(), e); }
        void addAll(Container<T> es) { es.stuff.forEach(this.stuff::put); }
        void clear() { stuff.clear(); }
        IResult<T> getResult() { return new Result<>(stuff.values()); }
    }

    @SuppressWarnings("WeakerAccess")
    public static class Accumulator<T extends Serializable> implements Serializable {
        private final Container<T> container = new Container<>();
        void add(Foo<T> e) { container.add(e); }
        void addAll(Accumulator<T> es) { container.addAll(es.container); }
        void clear() { container.clear(); }
        IResult<T> getResult() { return container.getResult(); }
    }

    public static abstract class AbstractEnlister<T extends Serializable>
            extends AggregateFunction<IResult<T>, Accumulator<T>>
    {
        @Override
        public Accumulator<T> createAccumulator() { return new Accumulator<>(); }
        @Override
        public IResult<T> getValue(Accumulator<T> acc) { return acc.getResult(); }
        @SuppressWarnings("unused")
        public void merge(Accumulator<T> acc, Iterable<Accumulator<T>> it) {
            for (Accumulator<T> otherAcc : it) {
                acc.addAll(otherAcc);
            }
        }
        @SuppressWarnings("unused")
        public void resetAccumulator(Accumulator<T> acc) { acc.clear(); }
        /*@Override
        public TypeInformation<IResult<T>> getResultType() {
            return TypeInformation.of(new TypeHint<IResult<T>>(){});
        }
        @Override
        public TypeInformation<Accumulator<T>> getAccumulatorType() {
            return TypeInformation.of(new TypeHint<Accumulator<T>>(){});
        }*/
    }

    public static class Bar implements Foo<String> {
        private final String s;
        Bar(String s) { this.s = s; }
        @Override
        public String sayHi() { return s; }
    }

    public static class Enlister extends AbstractEnlister<String> {
        @SuppressWarnings("unused")
        public void accumulate(Accumulator<String> acc, String a, String b) { acc.add(new Bar(a + ":" + b)); }
    }
}
