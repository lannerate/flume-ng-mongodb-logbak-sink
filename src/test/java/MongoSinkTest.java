
import com.mongodb.Mongo;
import org.apache.flume.*;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.riderzen.flume.sink.MongoSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.*;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: hui.zhang
 * Date: 14-9-13
 * Time: 下午4:23
 * To change this template use File | Settings | File Templates.
 */
@Test(singleThreaded = true, threadPoolSize = 1)
public class MongoSinkTest {
    private static Logger logger = LoggerFactory.getLogger(MongoSinkTest.class);
    private static Mongo mongo;
    public static final String DBNAME = "myDb";

    private static Context ctx = new Context();
    private static Channel channel;


    @BeforeMethod(groups = {"dev"})
    public static void setup() throws UnknownHostException {
        mongo = new Mongo("localhost", 27017);

        Map<String, String> ctxMap = new HashMap<String, String>();
        ctxMap.put(MongoSink.HOST, "localhost");
        ctxMap.put(MongoSink.PORT, "27017");
        ctxMap.put(MongoSink.DB_NAME, "test_events");
        ctxMap.put(MongoSink.COLLECTION, "test_log");
        ctxMap.put(MongoSink.BATCH_SIZE, "100");

        ctx.putAll(ctxMap);

        Context channelCtx = new Context();
        channelCtx.put("capacity", "1000000");
        channelCtx.put("transactionCapacity", "1000000");
        channel = new MemoryChannel();
        Configurables.configure(channel, channelCtx);
    }

  /*  @AfterMethod(groups = {"dev"})
    public static void tearDown() {
        mongo.dropDatabase(DBNAME);
        mongo.dropDatabase("test_events");
        mongo.dropDatabase("dynamic_db");
        mongo.close();
    }*/

   /* @Test(groups = "dev", invocationCount = 1)
    public void sinkDynamicTest() throws EventDeliveryException, InterruptedException {

        ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.dynamic.name());
        MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);

        sink.setChannel(channel);
        sink.start();

        JSONObject msg = new JSONObject();
        msg.put("age", 11);
        msg.put("birthday", new Date().getTime());

        Transaction tx;

        for (int i = 0; i < 100; i++) {
            tx = channel.getTransaction();
            tx.begin();
            msg.put("name", "test" + i);
            JSONObject header = new JSONObject();
            header.put(MongoSink.COLLECTION, "my_events");

            Event e = EventBuilder.withBody(msg.toJSONString().getBytes(), header);
            channel.put(e);
            tx.commit();
            tx.close();
        }
        sink.process();
        sink.stop();

        for (int i = 0; i < 10; i++) {
            msg.put("name", "test" + i);

            System.out.println("i = " + i);

            DB db = mongo.getDB("test_events");
            DBCollection collection = db.getCollection("my_events");
            DBCursor cursor = collection.find(new BasicDBObject(msg));
            assertTrue(cursor.hasNext());
            DBObject dbObject = cursor.next();
            assertNotNull(dbObject);
            assertEquals(dbObject.get("name"), msg.get("name"));
            assertEquals(dbObject.get("age"), msg.get("age"));
            assertEquals(dbObject.get("birthday"), msg.get("birthday"));
        }
    }

    @Test(groups = "dev")
    public void sinkDynamicTest2() throws EventDeliveryException, InterruptedException {
        ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.dynamic.name());
        MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);

        sink.setChannel(channel);
        sink.start();

        JSONObject msg = new JSONObject();
        msg.put("age", 11);
        msg.put("birthday", new Date().getTime());

        Transaction tx;

        for (int i = 0; i < 100; i++) {
            tx = channel.getTransaction();
            tx.begin();
            msg.put("name", "test" + i);
            JSONObject header = new JSONObject();
            header.put(MongoSink.COLLECTION, "my_events" + i % 10);

            Event e = EventBuilder.withBody(msg.toJSONString().getBytes(), header);
            channel.put(e);
            tx.commit();
            tx.close();
        }
        sink.process();
        sink.stop();

        for (int i = 0; i < 10; i++) {
            msg.put("name", "test" + i);

            System.out.println("i = " + i);

            DB db = mongo.getDB("test_events");
            DBCollection collection = db.getCollection("my_events" + i % 10);
            DBCursor cursor = collection.find(new BasicDBObject(msg));
            assertTrue(cursor.hasNext());
            DBObject dbObject = cursor.next();
            assertNotNull(dbObject);
            assertEquals(dbObject.get("name"), msg.get("name"));
            assertEquals(dbObject.get("age"), msg.get("age"));
            assertEquals(dbObject.get("birthday"), msg.get("birthday"));
        }
    }

    @Test(groups = "dev")
    public void sinkSingleModelTest() throws EventDeliveryException {
        ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.single.name());

        MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);

        sink.setChannel(channel);
        sink.start();

        Transaction tx = channel.getTransaction();
        tx.begin();
        JSONObject msg = new JSONObject();
        msg.put("name", "test");
        msg.put("age", 11);
        msg.put("birthday", new Date().getTime());

        Event e = EventBuilder.withBody(msg.toJSONString().getBytes());
        channel.put(e);
        tx.commit();
        tx.close();

        sink.process();
        sink.stop();

        DB db = mongo.getDB("test_events");
        DBCollection collection = db.getCollection("test_log");
        DBCursor cursor = collection.find(new BasicDBObject(msg));
        assertTrue(cursor.hasNext());
        DBObject dbObject = cursor.next();
        assertNotNull(dbObject);
        assertEquals(dbObject.get("name"), msg.get("name"));
        assertEquals(dbObject.get("age"), msg.get("age"));
        assertEquals(dbObject.get("birthday"), msg.get("birthday"));
    }

    @Test(groups = "dev")
    public void dbTest() {
        DB db = mongo.getDB(DBNAME);
        db.getCollectionNames();
        List<String> names = mongo.getDatabaseNames();

        assertNotNull(names);
        boolean hit = false;

        for (String name : names) {
            if (DBNAME.equals(name)) {
                hit = true;
                break;
            }
        }

        assertTrue(hit);
    }

    @Test(groups = "dev")
    public void collectionTest() {
        DB db = mongo.getDB(DBNAME);
        DBCollection myCollection = db.getCollection("myCollection");
        myCollection.save(new BasicDBObject(MapUtils.putAll(new HashMap(), new Object[]{"name", "leon", "age", 33})));
        myCollection.findOne();

        Set<String> names = db.getCollectionNames();

        assertNotNull(names);
        boolean hit = false;

        for (String name : names) {
            if ("myCollection".equals(name)) {
                hit = true;
                break;
            }
        }

        assertTrue(hit);
    }*/

    @Test(groups = "dev")
    public void autoWrapTest() throws EventDeliveryException {
//        ctx.put(MongoSink.AUTO_WRAP, Boolean.toString(false));
//        ctx.put(MongoSink.DB_NAME, "test_wrap");
        ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.dynamic.name());
        ctx.put(MongoSink.COLLECTION,"test_log");
        MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);

        sink.setChannel(channel);
        sink.start();

       /* String msg_body ="2014/05/05 08:07:54 INFO LINE[38] com.hzbank.heagle.server.security.ShiroSession - 0----------timeout : 7200000 \n";
//        String msg_body ="2014/05/04 09:01:27 INFO  LINE[739] com.hzbank.heagle.server.cswp.bcif.service.impl.CustomerInfoCommonServiceImpl - 客户号为：800080008记录存在 \n";
        Transaction tx = channel.getTransaction();
        tx.begin();
        Event e = EventBuilder.withBody(msg_body.getBytes());
        channel.put(e);
        tx.commit();
        tx.close();
        sink.process();
        sink.stop();*/

       try {
            InputStream fis = new FileInputStream(new File("H:\\heagle-all-0.log"));
            BufferedReader br = new BufferedReader(
                    new InputStreamReader(fis, Charset.forName("UTF-8")));
            String line;
            while ((line = br.readLine()) != null) {
                // Deal with the line
//                msg_body = line;
//                line+="\n";
                System.out.println(line);
                Transaction tx = channel.getTransaction();
                tx.begin();
                Event e = EventBuilder.withBody(line.getBytes());
                channel.put(e);
                tx.commit();
                tx.close();
                sink.process();
//                sink.stop();
//                Thread.sleep(100);

            }

            br.close();
            br = null;
            fis = null;

        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
//        final String PATT = "%d{yyyy/MM/dd HH:mm:ss} %-5p LINE[%L] %c - %msg%n";
     /*   String PATT = MongoSink.getLogPattern();
        Decoder decoder = DecoderLocal.getInstance(PATT);
        StaticLoggingEvent loggingEvent = (StaticLoggingEvent)decoder.decode(new String(msg_body));

        JSONObject msg = new JSONObject();
        System.out.println("logging Event="+loggingEvent.getThreadName());
        msg.put("timestamp",loggingEvent.getTimeStamp());
        msg.put("level",loggingEvent.getLevel().toString());
        msg.put("className",loggingEvent.getLoggerName());
        msg.put("message",loggingEvent.getMessage());
        msg.put("pid",MongoSink.getPid());
        msg.put("ip",MongoSink.getIp());

        JSONObject header = new JSONObject();
        header.put(MongoSink.COLLECTION, "my_events");
        header.put(MongoSink.DB_NAME, "dynamic_db");

        msg.put("name", "test");
        msg.put("age", 11);
        msg.put("birthday", new Date().getTime());*/



       /* DB db = mongo.getDB("dynamic_db");
        DBCollection collection = db.getCollection("my_events");
        DBCursor cursor = collection.find(new BasicDBObject(MongoSink.LOG_MESSAGE, msg));
        assertTrue(cursor.hasNext());
        DBObject dbObject = cursor.next();
        assertNotNull(dbObject);
        assertEquals(dbObject.get(MongoSink.LOG_MESSAGE), msg);
        mongo.dropDatabase("dynamic_db");*/
    }

   /* @Test(groups = "dev")
    public void sinkDynamicDbTest() throws EventDeliveryException {
        ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.dynamic.name());
        MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);

        sink.setChannel(channel);
        sink.start();

        JSONObject msg = new JSONObject();
        msg.put("age", 11);
        msg.put("birthday", new Date().getTime());

        Transaction tx;

        for (int i = 0; i < 10; i++) {
            tx = channel.getTransaction();
            tx.begin();
            msg.put("name", "test" + i);
            JSONObject header = new JSONObject();
            header.put(MongoSink.COLLECTION, "my_events");
            header.put(MongoSink.DB_NAME, "dynamic_db");

            Event e = EventBuilder.withBody(msg.toJSONString().getBytes(), header);
            channel.put(e);
            tx.commit();
            tx.close();
        }
        sink.process();
        sink.stop();

        for (int i = 0; i < 10; i++) {
            msg.put("name", "test" + i);

            System.out.println("i = " + i);

            DB db = mongo.getDB("dynamic_db");
            DBCollection collection = db.getCollection("my_events");
            DBCursor cursor = collection.find(new BasicDBObject(msg));
            assertTrue(cursor.hasNext());
            DBObject dbObject = cursor.next();
            assertNotNull(dbObject);
            assertEquals(dbObject.get("name"), msg.get("name"));
            assertEquals(dbObject.get("age"), msg.get("age"));
            assertEquals(dbObject.get("birthday"), msg.get("birthday"));
        }

    }

    @Test(groups = "dev")
    public void timestampNewFieldTest() throws EventDeliveryException {
        ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.dynamic.name());
        String tsField = "createdOn";
        ctx.put(MongoSink.TIMESTAMP_FIELD, tsField);
        MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);

        sink.setChannel(channel);
        sink.start();

        JSONObject msg = new JSONObject();
        msg.put("age", 11);
        msg.put("birthday", new Date().getTime());

        Transaction tx;

        for (int i = 0; i < 10; i++) {
            tx = channel.getTransaction();
            tx.begin();
            msg.put("name", "test" + i);
            JSONObject header = new JSONObject();
            header.put(MongoSink.COLLECTION, "my_events");
            header.put(MongoSink.DB_NAME, "dynamic_db");

            Event e = EventBuilder.withBody(msg.toJSONString().getBytes(), header);
            channel.put(e);
            tx.commit();
            tx.close();
        }
        sink.process();
        sink.stop();

        for (int i = 0; i < 10; i++) {
            msg.put("name", "test" + i);

            System.out.println("i = " + i);

            DB db = mongo.getDB("dynamic_db");
            DBCollection collection = db.getCollection("my_events");
            DBCursor cursor = collection.find(new BasicDBObject(msg));
            assertTrue(cursor.hasNext());
            DBObject dbObject = cursor.next();
            assertNotNull(dbObject);
            assertEquals(dbObject.get("name"), msg.get("name"));
            assertEquals(dbObject.get("age"), msg.get("age"));
            assertEquals(dbObject.get("birthday"), msg.get("birthday"));
            assertTrue(dbObject.get(tsField) instanceof Date);
        }

    }

    @Test(groups = "dev")
    public void timestampExistingFieldTest() throws EventDeliveryException, ParseException {
        ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.dynamic.name());
        String tsField = "createdOn";
        ctx.put(MongoSink.TIMESTAMP_FIELD, tsField);
        MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);

        sink.setChannel(channel);
        sink.start();

        JSONObject msg = new JSONObject();
        msg.put("age", 11);
        msg.put("birthday", new Date().getTime());
        String dateText = "2013-02-19T14:20:53+08:00";
        msg.put(tsField, dateText);

        Transaction tx;

        for (int i = 0; i < 10; i++) {
            tx = channel.getTransaction();
            tx.begin();
            msg.put("name", "test" + i);
            JSONObject header = new JSONObject();
            header.put(MongoSink.COLLECTION, "my_events");
            header.put(MongoSink.DB_NAME, "dynamic_db");

            Event e = EventBuilder.withBody(msg.toJSONString().getBytes(), header);
            channel.put(e);
            tx.commit();
            tx.close();
        }
        sink.process();
        sink.stop();

        msg.put(tsField, MongoSink.dateTimeFormatter.parseDateTime(dateText).toDate());
        for (int i = 0; i < 10; i++) {
            msg.put("name", "test" + i);

            System.out.println("i = " + i);

            DB db = mongo.getDB("dynamic_db");
            DBCollection collection = db.getCollection("my_events");
            DBCursor cursor = collection.find(new BasicDBObject(msg));
            assertTrue(cursor.hasNext());
            DBObject dbObject = cursor.next();
            assertNotNull(dbObject);
            assertEquals(dbObject.get("name"), msg.get("name"));
            assertEquals(dbObject.get("age"), msg.get("age"));
            assertEquals(dbObject.get("birthday"), msg.get("birthday"));
            assertTrue(dbObject.get(tsField) instanceof Date);
            System.out.println("ts = " + dbObject.get(tsField));
        }

    }

    @Test(groups = "dev")
    public void upsertTest() throws EventDeliveryException, ParseException {
        ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.dynamic.name());
        String tsField = "createdOn";
        ctx.put(MongoSink.TIMESTAMP_FIELD, tsField);
        MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);

        sink.setChannel(channel);
        sink.start();

        JSONObject msg = new JSONObject();
        msg.put("age", 11);
        msg.put("birthday", new Date().getTime());
        String dateText = "2013-02-19T14:20:53+08:00";
        msg.put(tsField, dateText);

        Transaction tx;

        for (int i = 0; i < 10; i++) {
            tx = channel.getTransaction();
            tx.begin();
            msg.put("_id", 1111 + i);
            msg.put("name", "test" + i);
            JSONObject header = new JSONObject();
            header.put(MongoSink.COLLECTION, "my_events");
            header.put(MongoSink.DB_NAME, "dynamic_db");

            Event e = EventBuilder.withBody(msg.toJSONString().getBytes(), header);
            channel.put(e);
            tx.commit();
            tx.close();
        }
        sink.process();
        sink.stop();

        for (int i = 0; i < 10; i++) {
            tx = channel.getTransaction();
            tx.begin();
            msg.put("_id", 1111 + i);
            msg.put("name", "test" + i * 10);
            JSONObject header = new JSONObject();
            header.put(MongoSink.COLLECTION, "my_events");
            header.put(MongoSink.DB_NAME, "dynamic_db");
            header.put(MongoSink.OPERATION, MongoSink.OP_UPSERT);

            Event e = EventBuilder.withBody(msg.toJSONString().getBytes(), header);
            channel.put(e);
            tx.commit();
            tx.close();
        }
        sink.process();
        sink.stop();

        msg.put(tsField, MongoSink.dateTimeFormatter.parseDateTime(dateText).toDate());
        for (int i = 0; i < 10; i++) {
            System.out.println("i = " + i);

            DB db = mongo.getDB("dynamic_db");
            DBCollection collection = db.getCollection("my_events");
            DBCursor cursor = collection.find(BasicDBObjectBuilder.start().add("_id", 1111 + i).get());
            assertTrue(cursor.hasNext());
            DBObject dbObject = cursor.next();
            assertNotNull(dbObject);
            assertEquals(dbObject.get("name"), "test" + i * 10);
            assertEquals(dbObject.get("age"), msg.get("age"));
            assertEquals(dbObject.get("birthday"), msg.get("birthday"));
            assertTrue(dbObject.get(tsField) instanceof Date);
            System.out.println("ts = " + dbObject.get(tsField));
            System.out.println("_id = " + dbObject.get("_id"));
        }

    }

    @Test
    public static void sandbox() throws EventDeliveryException {
        JSONObject msg = new JSONObject();
        JSONObject set = new JSONObject();
        set.put("pid", "274");
        set.put("fac", "missin-do");
        msg.put("$set", set);

        JSONObject inc = new JSONObject();
        inc.put("sum", 1);

        msg.put("$inc", inc);
        msg.put("_id", "111111111111111111111111111");
        msg.put("pid", "111111111111111111111111111");

        ctx.put(MongoSink.MODEL, MongoSink.CollectionModel.dynamic.name());
        String tsField = "createdOn";
        ctx.put(MongoSink.TIMESTAMP_FIELD, tsField);
        MongoSink sink = new MongoSink();
        Configurables.configure(sink, ctx);

        sink.setChannel(channel);
        sink.start();

        Transaction tx;

        tx = channel.getTransaction();
        tx.begin();

        JSONObject header = new JSONObject();
        header.put(MongoSink.COLLECTION, "my_eventsjj");
        header.put(MongoSink.DB_NAME, "dynamic_dbjj");
        header.put(MongoSink.OPERATION, MongoSink.OP_UPSERT);

        Event e = EventBuilder.withBody(msg.toJSONString().getBytes(), header);
        channel.put(e);
        tx.commit();
        tx.close();
        sink.process();
        sink.stop();
    }*/
}
