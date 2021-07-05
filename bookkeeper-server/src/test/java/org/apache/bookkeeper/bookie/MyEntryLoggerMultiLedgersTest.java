package org.apache.bookkeeper.bookie;


import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.EntryLogger;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.utils.MyEntryLoggerUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

@RunWith(value = Parameterized.class)
public class MyEntryLoggerMultiLedgersTest {

    final List<File> tempDirs = new ArrayList<>();

    private File rootDir;
    private File curDir;
    private ServerConfiguration conf;
    private LedgerDirsManager dirsMgr;
    private EntryLogger entryLogger;

    //This boolean is used for enable new EntryLog per ledger in the configuration
    private boolean initialEntryLogPerLedgerEnabled;
    private int numOfActiveLedgers;
    private int numEntries;


    @Parameterized.Parameters
    public static Collection<?> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {15,10,true},
                {15,10,false}



        });
    }

    public MyEntryLoggerMultiLedgersTest(int numOfActiveLedgers, int numEntries, boolean initialEntryLogPerLedgerEnabled){
        this.initialEntryLogPerLedgerEnabled = initialEntryLogPerLedgerEnabled;
        this.numOfActiveLedgers = numOfActiveLedgers;
        this.numEntries =  numEntries;
    }

    //used for Bookie directory creation
    File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        //stored for end test deletion
        tempDirs.add(dir);
        return dir;
    }

    /**
     * Standard creation of a Bookie and EntryLogger
     */
    @Before
    public void setUp() throws Exception {

        //temp directory
        this.rootDir = createTempDir("myTest", ".dir");
        this.curDir = Bookie.getCurrentDirectory(rootDir);
        Bookie.checkDirectoryStructure(curDir);
        //this class make a standard server configuration
        this.conf = TestBKConfiguration.newServerConfiguration();
        this.conf.setEntryLogPerLedgerEnabled(initialEntryLogPerLedgerEnabled);
        this.dirsMgr = new LedgerDirsManager(
                conf,
                new File[] { rootDir },
                new DiskChecker(
                        conf.getDiskUsageThreshold(),
                        conf.getDiskUsageWarnThreshold()));
        this.entryLogger = new EntryLogger(conf, dirsMgr);
    }

    @After
    public void tearDown() throws Exception {
        if (null != this.entryLogger) {
            entryLogger.shutdown();
        }

        for (File dir : tempDirs) {
            FileUtils.deleteDirectory(dir);
        }
        tempDirs.clear();
    }

    /*
    * Also assert if EntryLogId it's correct
    * */

    private long[][] createAndFillLedgers() throws IOException {
        long[][] positions = new long[numOfActiveLedgers][];
        for (int i = 0; i < numOfActiveLedgers; i++) {
            positions[i] = new long[numEntries];
        }
        //add entries to the ledgers
        for (int j = 0; j < numEntries; j++) {
            for (int i = 0; i < numOfActiveLedgers; i++) {
                positions[i][j] = entryLogger.addEntry((long) i, MyEntryLoggerUtils.generateEntry(i, j));
                long entryLogId = (positions[i][j] >> 32L);

                //Testing if correct entryLogId
                // initialEntryLogPerLedgerEnabled = false expected EntryLogId 0 (only one creation)
                if (initialEntryLogPerLedgerEnabled) {
                    assertEquals("EntryLogId for ledger: " + i, i, entryLogId);
                } else {
                    assertEquals("EntryLogId for ledger: " + i, 0, entryLogId);
                }
            }
        }
        return positions;
    }



    /*
    * Test EntryLogger.readEntry on multiple ledgers
    * */

    @Test
    public void testReadEntryMultipleLedgers() throws IOException {
        long[][] positions = createAndFillLedgers();
        for (int j = 0; j < numEntries; j++) {
            for (int i = 0; i < numOfActiveLedgers; i++) {
                String expectedValue = MyEntryLoggerUtils.generateDataString(i,j);
                ByteBuf buf = entryLogger.readEntry(i, j, positions[i][j]);
                long ledgerId = buf.readLong();
                long entryId = buf.readLong();
                byte[] data = new byte[buf.readableBytes()];
                buf.readBytes(data);
                assertEquals("LedgerId ", i, ledgerId);
                assertEquals("EntryId ", j, entryId);
                assertEquals("Entry Data ", expectedValue, new String(data));
            }
        }
    }


}
