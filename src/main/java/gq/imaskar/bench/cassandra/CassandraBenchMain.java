package gq.imaskar.bench.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 *
 * @author imaskar
 */
@State(Scope.Benchmark)
public class CassandraBenchMain {

  private static final String TEXT = "Wake up, Neo. We have updated our privacy policy.";

  @State(Scope.Thread)
  public static class CassState {

    Session session;
    Cluster cluster;
    int i = 0;

    public CassState() {
    }
    
    @Setup(Level.Trial)
    public void setup(){
      cluster = Cluster.builder() 
          .addContactPoint("127.0.0.1")
          .build();
      session = cluster.connect();
      
    }

    @TearDown(Level.Trial)
    public void teardown() {
      session.close();
      cluster.close();
    }

  }

  @Benchmark
  public void write(CassState state) {
    state.session.execute("insert into test.test (id,data) values (?,?)", state.i++, TEXT);
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(CassandraBenchMain.class.getSimpleName())
        .timeUnit(TimeUnit.MICROSECONDS)
        .warmupTime(TimeValue.seconds(5))
        .warmupIterations(2)
        .measurementTime(TimeValue.seconds(10))
        .measurementIterations(5)
        .forks(5)
        .threads(1)
        .mode(Mode.Throughput)
        .shouldFailOnError(true)
        .shouldDoGC(true)
        .build();

    new Runner(opt).run();
  }

}
