package org.apache.bookkeeper;

import io.netty.buffer.ByteBuf;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(value = Parameterized.class)
public class MyEntryLoggerTest {

    final List<File> tempDirs = new ArrayList<>();

    private File rootDir;
    private File curDir;
    private ServerConfiguration conf;
    private LedgerDirsManager dirsMgr;
    private EntryLogger entryLogger;

    private long ledgerId;
    private long entryId;
    private boolean validateEntry;

    @Parameterized.Parameters
    public static Collection<?> getTestParameters() {
        return Arrays.asList(new Object[][]{
                {0L,0L,true},
                {1L,0L,true},
                {-1L,0L,true},
                {0L,-1L,true},
                //{0L,0L,false}, as expected the test fail (assertion)because there are no validation
                //{0L,-1L,false}


        });
    }

    public MyEntryLoggerTest(long ledgerId,long entryId ,boolean validateEntry){
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.validateEntry = validateEntry;
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
     * Testing directly EntryLogger.InternalReadEntry
     * Because parameter are based on boundary-value analysis addEntry and readEntry must throw an Exception
     * (key must be >= 0 from documentation)
     * */
    @Test
    public void testInternalReadEntryGoodPosition() throws IOException {
        testInternalReadEntry(this.ledgerId,this.entryId);
    }


    /*
    * add Entry in a different position respect read
    * if validateEntry == true expected Exception.class throw by internalReadEntry
    * if validateEntry == false AssertionError
    * */
    @Test(expected = Exception.class)
    public void testInternalReadEntryWrongPosition() throws Exception {
        testInternalReadEntry(this.ledgerId + 1L,this.entryId+ 1L);
    }

    private void testInternalReadEntry(long ledgerId,long entryId) throws IOException {
        //add entry
        long location = 0;

        try {
            location = entryLogger.addEntry(ledgerId, MyEntryLoggerUtils.generateEntry(ledgerId,entryId));
        } catch (Exception e) {
            assertEquals("Keys and values must be >= 0",e.getMessage());
        }

        ByteBuf buf = null;
        try {
            buf = entryLogger.internalReadEntry(this.ledgerId, this.entryId, location,this.validateEntry);
            long readLedgerId = buf.readLong();
            long readEntryId = buf.readLong();
            byte[] data = new byte[buf.readableBytes()];
            buf.readBytes(data);
            String expectedValue = MyEntryLoggerUtils.generateDataString(this.ledgerId,this.entryId);
            assertEquals("LedgerId ", this.ledgerId, readLedgerId);
            assertEquals("EntryId ", this.entryId, readEntryId);
            assertEquals("Entry Data ", expectedValue, new String(data));
        } catch (IllegalArgumentException e) {
            assertEquals("Negative position",e.getMessage());
        } catch (IOException e){
            throw e;
        }



    }



    /*
     * From documentation: A new entry log file is created once the bookie starts
     * or the older entry log file reaches the entry log size threshold
     * */
    @Test
    public void testNewEntryLogCreationForSizeThreshold(){

    }
}
