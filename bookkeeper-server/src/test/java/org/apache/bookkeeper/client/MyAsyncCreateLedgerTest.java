package org.apache.bookkeeper.client;



import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(value = Parameterized.class)
public class MyAsyncCreateLedgerTest extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(MyAsyncCreateLedgerTest.class);

    private BookKeeper.DigestType digestType = BookKeeper.DigestType.CRC32;
    private static final byte[] PASSWORD = "test".getBytes(StandardCharsets.UTF_8);
    private static final int numberOfBookie = 5;

    private int ensSize;
    private int writeQuorumSize;
    private int ackQuorumSize;
    private AsyncCallback.CreateCallback cb;
    private LedgerHandle ledgerHandle;
    private AtomicBoolean error;
    private BKException bkException;


    @Parameterized.Parameters
    public static Collection<?> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {-1,3,2},
                {6,6,4},
                {3,2,1}

                //{0L,0L,false}, as expected the test fail (assertion)because there are no validation
                //{0L,-1L,false}


        });
    }

    public MyAsyncCreateLedgerTest(int ensSize, int writeQuorumSize, int ackQuorumSize){
        super(numberOfBookie);
        this.ensSize = ensSize;
        this.writeQuorumSize = writeQuorumSize;
        this.ackQuorumSize = ackQuorumSize;


    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.ledgerHandle = null;
        this.error = new AtomicBoolean(false);

        this.cb = new AsyncCallback.CreateCallback() {
            @Override
            public void createComplete(int rc, LedgerHandle lh, Object ctx) {
                LOG.info("----------CALLBACK------------");
                if (BKException.Code.OK == rc) {
                    bkException = BKException.create(rc);
                    synchronized (ctx){
                        ledgerHandle = lh;
                        ctx.notify();
                    }
                } else {
                    bkException = BKException.create(rc);

                    synchronized (ctx){
                        error.set(true);
                        ctx.notify();
                    }
                }
            }
        };


    }

    @After
    public void tearDown() throws Exception{
        super.tearDown();
    }

    /*
    * Expected IllegalArgumentException:
    * - if ensemble size is negative (msg = Illegal Capacity)
    * - if ack quorum size greater then write one (msg = Write quorum must be larger than ack quorum)
    * -> The write quorum must be larger than the ack quorum (from documentation)
    * */
    @Test
    public void test(){
        Object lock = new Object();
        LOG.info("----------START------------");
        try {
            bkc.asyncCreateLedger(ensSize,writeQuorumSize,ackQuorumSize,digestType,
                    PASSWORD,cb,lock,null);

            //wait callback
            synchronized (lock){
                while (ledgerHandle == null && !error.get()) {
                    try {
                        LOG.info("----------WAIT------------");

                        lock.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    LOG.info("----------EXIT------------");
                }
            }

            //assert good creation of ledgerHandle
            if (ledgerHandle != null){
                LedgerMetadata data = ledgerHandle.getLedgerMetadata();
                assertEquals(data.getEnsembleSize(),ensSize);
                assertEquals(data.getWriteQuorumSize(),writeQuorumSize);
                assertEquals(data.getAckQuorumSize(),ackQuorumSize);

            }else { //error on callback
                /*
                 * If ensemble size is greater than number of bookies expected:BKNotEnoughBookiesException
                 * */
                LOG.error("Find error:" + bkException.getMessage());
                assertEquals(BKException.BKNotEnoughBookiesException.class,bkException.getClass());
            }

        }catch (IllegalArgumentException e){
            LOG.error("IllegalArgumentException with msg: "+e.getMessage());
            if (!(e.getMessage().contains("Illegal Capacity") ||
                    e.getMessage().contains("Write quorum must be larger than ack quorum"))){
                fail("Unexpected IllegalArgumentException");
            }

        }

        LOG.info("----------END------------");

    }


}
