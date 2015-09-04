package edu.illinois.adsc;

import backtype.storm.utils.DisruptorQueue;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.MultiThreadedClaimStrategy;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * Hello world!
 *
 */
public class Benchmark
{
    private final static int TIMEOUT = 1000; // MS
    private final static int PRODUCER_NUM = 4;

    @Option(name="--time", aliases = {"-h"}, usage = "set evaluation time")
    private int _executeTime;

    @Option(name = "--help", aliases = {"-h"}, usage = "help")
    private boolean _help;

    public void testMain( String[] args )
    {
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

        DisruptorQueue queue = createQueue("MyQueue",TIMEOUT);

        Consumer consumer = new Consumer(queue, handle);

        Producer producer = new Producer(queue);



        try {
            Report report = execute(producer, consumer, handle ,_executeTime);
            report.print();

        }
        catch (InterruptedException e){

        }
    }

    public static void main(String[] args) throws Exception {
        new Benchmark().testMain(args);
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
            try {
                while(true) {
                    queue.consumeBatchWhenAvailable(handler);
                }
            }catch(RuntimeException e) {
                //break
            }
        }
    }

    private static class Producer implements Runnable {
        private DisruptorQueue queue;

        Producer(DisruptorQueue queue) {
            this.queue = queue;
        }

        public void run() {
            try {
                while (true) {
                    queue.publish(System.currentTimeMillis(), false);
                }
            } catch (InsufficientCapacityException e) {
                return;
            }
        }
    }

    interface Reporter {
        public double processLatency();
        public double processThroughput();
    }

    private static class Handle implements EventHandler<Object>, Reporter {
        private long totalTicks;
        private long count;
        private boolean started;
        private long startTime;
        public Handle () {
            totalTicks = 0;
            count = 0;
            started = false;
        }

        public void onEvent(Object obj, long sequence, boolean endOfBatch) {
            if(!started) {
                started = true;
                startTime = System.currentTimeMillis();
            }
            totalTicks += System.currentTimeMillis() - (Long)obj;
            count ++;
        }

        public double processLatency() {
            return (float)totalTicks / count;
        }

        public double processThroughput() {
            return count / (System.currentTimeMillis() - startTime) * 1000;
        }
    }

    static private Report execute(Runnable producer, Runnable consumer, Reporter reporter,int executeSeconds)
            throws InterruptedException {

        Thread[] producerThreads = new Thread[PRODUCER_NUM];
        for (int i = 0; i < PRODUCER_NUM; i++) {
            producerThreads[i] = new Thread(producer);
            producerThreads[i].start();
        }

        Thread consumerThread = new Thread(consumer);
        consumerThread.start();
        Thread.sleep(executeSeconds);
        for (int i = 0; i < PRODUCER_NUM; i++) {
            producerThreads[i].interrupt();
        }
        consumerThread.interrupt();

        for (int i = 0; i < PRODUCER_NUM; i++) {
            producerThreads[i].join(TIMEOUT);
            if (producerThreads[i].isAlive()) {
                System.out.print("producer " + i + " is still alive" + producerThreads[i].isAlive());
                assert (false);
            }
        }
        consumerThread.join(TIMEOUT);
        if (consumerThread.isAlive()) {
            System.out.print("consumer is still alive" + consumerThread.isAlive());
            assert (false);
        }

        Report ret = new Report();
        ret.averageDelay = reporter.processLatency();
        ret.throughput = reporter.processThroughput();

        return ret;

    }
    private static DisruptorQueue createQueue(String name, int queueSize) {
        return new DisruptorQueue(name, new MultiThreadedClaimStrategy(
                queueSize), new BlockingWaitStrategy(), 10L);
    }

    private static class Report {
        public double averageDelay;
        public double throughput;
        public void print() {
            System.out.println("Execution Delay: " + averageDelay + "\tThroughput:" + throughput);
        }
    }
}
