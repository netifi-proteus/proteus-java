package io.netifi.proteus.collections;

import io.netifi.proteus.broker.info.Destination;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Objects;

public class IndexableStoreTest {
  IndexableStore.KeyHasher<String> destinationHasher =
      new IndexableStore.KeyHasher<String>() {
        @Override
        public long hash(String... t) {
          String group = t[0];
          String destination = t[1];
          return pack(group.hashCode(), destination.hashCode());
        }

        private long pack(int group, int destination) {
          return (((long) group) << 32) | (destination & 0xffffffffL);
        }
      };

  @Test
  public void testAdd() {
    IndexableStore<String, IndexableStore.KeyHasher<String>, Destination> store =
        new IndexableStore<>(destinationHasher);

    Destination destination = Destination.newBuilder().setDestination("d1").setGroup("g1").build();

    IndexableStore.Entry<Destination> entry = store.put(destination, "g1", "d1");

    entry.add("group", "g1").add("service", "s1").add("version", "1.0.1");

    List<IndexableStore.Entry<Destination>> query = store.query("service", "s1");

    Destination destination1 = query.get(0).get();

    Assert.assertEquals(destination.getDestination(), destination1.getDestination());

    List<IndexableStore.Entry<Destination>> query1 = store.query("service", "s1", "group", "g1");
    Destination destination2 = query1.get(0).get();

    Assert.assertEquals(destination2.getDestination(), destination.getDestination());

    List<IndexableStore.Entry<Destination>> query2 = store.query("foo", "bar", "service", "s1");

    Assert.assertTrue(query2.isEmpty());

    List<IndexableStore.Entry<Destination>> group =
        store.query("group", "you will not find this group");

    Assert.assertTrue(group.isEmpty());

    List<IndexableStore.Entry<Destination>> query4 =
        store.query("service", "s1", "group", "you will not find this group");

    Assert.assertTrue(query4.isEmpty());
    
  }
  
  @Test
  public void testAddAndRemove() {
    IndexableStore<String, IndexableStore.KeyHasher<String>, Destination> store =
        new IndexableStore<>(destinationHasher);
    
    Destination destination = Destination.newBuilder().setDestination("d1").setGroup("g1").build();
    
    IndexableStore.Entry<Destination> entry = store.put(destination, "g1", "d1");
    
    entry.add("group", "g1").add("service", "s1").add("version", "1.0.1");
    
    List<IndexableStore.Entry<Destination>> query = store.query("service", "s1");
    
    Destination destination1 = query.get(0).get();
    
    Assert.assertEquals(destination.getDestination(), destination1.getDestination());
    
    Assert.assertTrue(store.containsTag("service", "s1"));
    Assert.assertFalse(store.containsTag("not found", "s1"));
    Assert.assertTrue(store.containsTags("service", "s1", "group", "g1"));
    Assert.assertFalse(store.containsTags("service", "s1", "group", "not here"));
    
    List<IndexableStore.Entry<Destination>> query1 = store.query("service", "s1", "group", "g1");
    Destination destination2 = query1.get(0).get();
    
    Assert.assertEquals(destination2.getDestination(), destination.getDestination());
    
    List<IndexableStore.Entry<Destination>> query2 = store.query("foo", "bar", "service", "s1");
    
    Assert.assertTrue(query2.isEmpty());
    
    List<IndexableStore.Entry<Destination>> group =
        store.query("group", "you will not find this group");
    
    Assert.assertTrue(group.isEmpty());
    
    List<IndexableStore.Entry<Destination>> query4 =
        store.query("service", "s1", "group", "you will not find this group");
    
    Assert.assertTrue(query4.isEmpty());
    
    store.remove("g1", "d1");
  
    List<IndexableStore.Entry<Destination>> query3 = store.query("group", "g1");
    
    Assert.assertTrue(query3.isEmpty());
  }

  @Test
  public void testAddMultiple() {
    IndexableStore<String, IndexableStore.KeyHasher<String>, Destination> store =
        new IndexableStore<>(destinationHasher);

    for (int g = 0; g < 100; g++) {
      String group = "g" + g;

      for (int d = 0; d < 1_000; d++) {
        String destination = "d" + d;

        Destination _d =
            Destination.newBuilder().setDestination(destination).setGroup(group).build();

        IndexableStore.Entry<Destination> entry = store.put(_d, group, destination);

        entry.add("group", group);

        for (int s = 0; s < 3; s++) {
          String service = "s" + s;
          entry.add("service", service);
          entry.add("service", group);
        }
      }
    }

    List<IndexableStore.Entry<Destination>> query = store.query("service", "s1");
    Assert.assertFalse(query.isEmpty());
    long start = System.nanoTime();
    System.out.println("size " + query.size());
    long stop = System.nanoTime();

    System.out.println("time in nanos " + (stop - start));

    start = System.nanoTime();
    List<IndexableStore.Entry<Destination>> query1 = store.query("service", "g50");
    System.out.println("size " + query1.size());
    stop = System.nanoTime();
    System.out.println("time in nanos " + (stop - start));
  }

  @Test
  public void testAddAndQuery1MItems() {
    IndexableStore<String, IndexableStore.KeyHasher<String>, Destination> store =
        new IndexableStore<>(destinationHasher);

    for (int g = 0; g < 100; g++) {
      String group = "g" + g;

      for (int d = 0; d < 1_000; d++) {
        String destination = "d" + d;

        Destination _d =
            Destination.newBuilder().setDestination(destination).setGroup(group).build();

        IndexableStore.Entry<Destination> entry = store.put(_d, group, destination);

        entry.add("group", group);

        for (int s = 0; s < 3; s++) {
          String service = "s" + s;
          entry.add("service", service);
          entry.add("service", group);
        }
      }
    }

    List<IndexableStore.Entry<Destination>> query = store.query("service", "s1");
    Assert.assertFalse(query.isEmpty());
    long start = System.nanoTime();
    System.out.println("size " + query.size());
    long stop = System.nanoTime();

    System.out.println("time in nanos " + (stop - start));

    for (int i = 0; i < 1_000_000; i++) {
      List<IndexableStore.Entry<Destination>> query1 = store.query("service", "g50");
    }
  }
  
  @Test
  public void testAddAndQuery1MItemsWithMultiValues() {
    IndexableStore<String, IndexableStore.KeyHasher<String>, Destination> store =
        new IndexableStore<>(destinationHasher);
    
    for (int g = 0; g < 100; g++) {
      String group = "g" + g;
      
      for (int d = 0; d < 1_000; d++) {
        String destination = "d" + d;
        
        Destination _d =
            Destination.newBuilder().setDestination(destination).setGroup(group).build();
        
        IndexableStore.Entry<Destination> entry = store.put(_d, group, destination);
        
        entry.add("group", group);
        
        for (int s = 0; s < 3; s++) {
          String service = "s" + s;
          entry.add("service", service);
          entry.add("service", group);
        }
      }
    }
    
    List<IndexableStore.Entry<Destination>> query = store.query("service", "s1");
    Assert.assertFalse(query.isEmpty());
    long start = System.nanoTime();
    System.out.println("size " + query.size());
    long stop = System.nanoTime();
    
    System.out.println("time in nanos " + (stop - start));
    
    for (int i = 0; i < 1_000_000; i++) {
      List<IndexableStore.Entry<Destination>> query1 = store.query("service", "g50", "group", "g50");
      Assert.assertTrue(!query1.isEmpty());
    }
  }
  
}
