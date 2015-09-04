package edu.illinois.adsc;

import backtype.storm.utils.DisruptorQueue;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.MultiThreadedClaimStrategy;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.Serializable;
import java.util.Random;
import java.util.Vector;

/**
 * Hello world!
 */
public class Benchmark {
    @Option(name = "--runs", aliases = {"-r"}, usage = "set number of runs")
    private int _runs = 5; // MS


    @Option(name = "--time", aliases = {"-t"}, usage = "set report cycles in seconds")
    private int _reportCycles = 5;

    @Option(name = "--help", aliases = {"-h"}, usage = "help")
    private boolean _help;

    @Option(name = "--num-producer", aliases = {"-p"}, usage = "set producer number ")
    private int _producerNum = 3;

    @Option(name = "--tuple-size", aliases = {"-s"}, usage = "set tuple size ")
    private static int _tupleSize = 64;

    public void testMain(String[] args) {
        CmdLineParser parser = new CmdLineParser(this);
        parser.setUsageWidth(80);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            _help = true;
        }
        if (_help) {
            parser.printUsage(System.err);
            System.err.println();
            return;
        }

        Handle handle = new Handle();

        Monitor monitor =  handle;

        DisruptorQueue queue = createQueue("MyQueue", 16);

        Consumer consumer = new Consumer(queue, handle);

        Producer producer = new Producer(queue);

        Reporter reporter = new Reporter(monitor, _reportCycles);

        try {
           execute(producer, consumer, reporter, _reportCycles);
            reporter.printSummary();
            System.out.println("------------------------------------------------------");
            System.out.println("Test finishes, you can kill the program if it does not terminate.");


        } catch (InterruptedException e) {

        }
    }

    public static void main(String[] args) throws Exception {
        new Benchmark().testMain(args);
    }

    private static class Tuple implements Serializable {

        private Long [] values;

        private Long key;

        public Tuple(int length){
            values = new Long[length / Long.SIZE -1];
        }
        public void randomGen() {
            Random random = new Random();
            for(int i = 1 ; i< values.length; i++) {
                values[i] = random.nextLong();
            }
        }

        public void setKey(Long k) {
            key = k;
        }

        public Long getKey() {
            return key;
        }
    }

    private static class Consumer implements Runnable {
        public EventHandler handler;
        private DisruptorQueue queue;

        Consumer(DisruptorQueue queue, EventHandler handler) {
            this.handler = handler;
            this.queue = queue;
        }

        public void run() {
            queue.consumerStarted();
            while (true) {
                try {
                    queue.consumeBatchWhenAvailable(handler);
//                    queue.consumeBatch(handler);
                } catch (RuntimeException e) {
                    //break
                }
            }
        }
    }

    private static class Producer implements Runnable {
        private DisruptorQueue queue;
        private int count;

        Producer(DisruptorQueue queue) {
            this.queue = queue;
            count = 0;
        }

        public void run() {
            while (true) {
                try {
                    //queue.publish(0,false);
                    System.out.println("Before publish!");
                    Tuple tuple = new Tuple(_tupleSize);
                    System.out.println("published!" + count++);
                    tuple.randomGen();
                    tuple.setKey(System.currentTimeMillis());
                    queue.publish(System.currentTimeMillis(), true);
//                    System.out.println("Publish " + count++);
                } catch (InsufficientCapacityException e) {
                    System.err.println(e.getMessage());
                    return;
                }
            }
        }
    }

    interface Monitor {
        public Double processLatency();
        public double processThroughput();
        public void reset();
    }

    private static class Handle implements EventHandler<Object>, Monitor {
        private long totalTicks;
        private long count;
        private boolean started;
        private long startTime;

        public Handle() {
            totalTicks = 0;
            count = 0;
            started = false;
        }

        public void onEvent(Object obj, long sequence, boolean endOfBatch) {
            if (!started) {
                started = true;
                startTime = System.currentTimeMillis();
                count = 0;
                totalTicks = 0;
            }
            System.out.println("Consumped! count:" + count);

//            totalTicks += (Long)obj;
            Tuple tuple = (Tuple)obj;
            System.out.println("--->");
            try {
            totalTicks += System.currentTimeMillis() - tuple.getKey();
            System.out.println("Key:" + tuple.getKey());
            count++;
            }
            catch (Exception e) {
                System.err.println("Something happend!"+e.getCause());
            }
        }

        public Double processLatency() {
            return (double) totalTicks / count;
        }

        public double processThroughput() {
            return ((double) count / (System.currentTimeMillis() - startTime)) * 1000;
        }

        public void reset() {
            started = false;
        }

    }

    private void execute(Runnable producer, Runnable consumer, Runnable reporter, int executeSeconds)
            throws InterruptedException {

        Thread[] producerThreads = new Thread[_producerNum];
        for (int i = 0; i < _producerNum; i++) {
            producerThreads[i] = new Thread(producer);
            producerThreads[i].start();
        }

        Thread consumerThread = new Thread(consumer);
        consumerThread.start();

        Thread reporterThread = new Thread(reporter);
        reporterThread.start();


        Thread.sleep((long)((double)_runs + 1.5) * _reportCycles * 1000);
        reporterThread.interrupt();
        System.out.println("------------------------------------------------------");



        return;



    }

    private static DisruptorQueue createQueue(String name, int queueSize) {
        return new DisruptorQueue(name, new MultiThreadedClaimStrategy(
                queueSize), new BlockingWaitStrategy(), 16L);
    }

    private static class Reporter implements Runnable  {

        private int reportCycles;
        private Monitor monitor;

        private Vector<Double> latencies;
        private Vector<Double> throughputs;

        public Reporter(Monitor monitor, int reportCycles) {
            this.reportCycles = reportCycles;
            this.monitor = monitor;
            latencies = new Vector<Double>();
            throughputs = new Vector<Double>();
        }

        private void print() {
//            System.out.println(monitor.processLatency() + "ms\t\t" + monitor.processThroughput());
            double latency = monitor.processLatency();
            double throughput = monitor.processThroughput();
            System.out.format("%f\t\t%f\n", latency, throughput);
            latencies.add(latency);
            throughputs.add(throughput);
        }

        private void printHeader() {
            System.out.println("Execution Delay (ms)\tThroughput (tuples/s)");
        }

        public void printSummary() {
            System.out.println("Summary:");
            System.out.println("\t\tMIN\t\tMAX\t\tAVG");
            System.out.format("Latency   \t%f\t%f\t%f\n", getMin(latencies), getMin(latencies), getAvg(latencies) );
            System.out.format("Throughput\t%6.2f\t%6.2f\t%6.2f\n", getMin(throughputs), getMin(throughputs), getAvg(throughputs) );
        }

        private double getMin(Vector<Double> inputs){
            double ret = Double.MAX_VALUE;
            for(double i : inputs) {
                ret = Math.min(ret, i);
            }
            return ret;
        }

        private double getMax(Vector<Double> inputs){
            double ret = -1;
            for(double i : inputs) {
                ret = Math.max(ret, i);
            }
            return ret;
        }

        private double getAvg(Vector<Double> inputs){
            if(inputs.isEmpty())
                return 0;
            double ret = 0;
            for(double i : inputs) {
                ret += i;
            }
            return ret / inputs.size();
        }

        public void run() {
            try {
                printHeader();
                while(true){
                    Thread.sleep(reportCycles*1000);
                    print();
                }
            }
            catch (Exception e) {

            }
        }
    }
}
