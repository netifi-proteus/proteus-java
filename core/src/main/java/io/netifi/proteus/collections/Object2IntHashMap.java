package io.netifi.proteus.collections;


import io.netifi.proteus.util.Hashing;

import java.io.Serializable;
import java.util.*;
import java.util.function.ToIntFunction;

import static io.netifi.proteus.util.BitUtil.findNextPositivePowerOfTwo;
import static io.netifi.proteus.util.CollectionUtil.validateLoadFactor;


/**
 * {@link java.util.Map} implementation specialised for int values using open addressing and
 * linear probing for cache efficient access. The implementation is mirror copy of Int2ObjectHashMap
 * and it also relies on missing value concept from Int2IntHashMap
 *
 * @param <K> type of keys stored in the {@link java.util.Map}
 */
public class Object2IntHashMap<K>
    implements Map<K, Integer>, Serializable
{
    static final int MIN_CAPACITY = 8;
    
    private final float loadFactor;
    private final int missingValue;
    private int resizeThreshold;
    private int size;
    private final boolean shouldAvoidAllocation;
    
    private K[] keys;
    private int[] values;
    
    private ValueCollection valueCollection;
    private KeySet keySet;
    private EntrySet entrySet;
    
    /**
     * Construct a map with default capacity and load factor.
     *
     * @param missingValue value to be used as a null maker in the map
     */
    public Object2IntHashMap(final int missingValue)
    {
        this(MIN_CAPACITY, Hashing.DEFAULT_LOAD_FACTOR, missingValue);
    }
    
    /**
     * Construct a new map allowing a configuration for initial capacity and load factor.
     *
     * @param initialCapacity for the backing array
     * @param loadFactor      limit for resizing on puts
     * @param missingValue    value to be used as a null marker in the map
     */
    @SuppressWarnings("unchecked")
    public Object2IntHashMap(
        final int initialCapacity,
        final float loadFactor,
        final int missingValue)
    {
        this(initialCapacity, loadFactor, missingValue, true);
    }
    
    /**
     * Construct a new map allowing a configuration for initial capacity and load factor.
     * @param initialCapacity       for the backing array
     * @param loadFactor            limit for resizing on puts
     * @param missingValue          value to be used as a null marker in the map
     * @param shouldAvoidAllocation should allocation be avoided by caching iterators and map entries.
     */
    @SuppressWarnings("unchecked")
    public Object2IntHashMap(
        final int initialCapacity,
        final float loadFactor,
        final int missingValue,
        final boolean shouldAvoidAllocation)
    {
        validateLoadFactor(loadFactor);
        
        this.loadFactor = loadFactor;
        /* */ final int capacity = findNextPositivePowerOfTwo(Math.max(MIN_CAPACITY, initialCapacity));
        /* */ resizeThreshold = (int)(capacity * loadFactor);
        
        this.missingValue = missingValue;
        this.shouldAvoidAllocation = shouldAvoidAllocation;
        keys = (K[])new Object[capacity];
        values = new int[capacity];
        Arrays.fill(values, missingValue);
        
    }
    
    /**
     * Copy construct a new map from an existing one.
     *
     * @param mapToCopy for construction.
     */
    public Object2IntHashMap(final Object2IntHashMap<K> mapToCopy)
    {
        this.loadFactor = mapToCopy.loadFactor;
        this.resizeThreshold = mapToCopy.resizeThreshold;
        this.size = mapToCopy.size;
        this.missingValue = mapToCopy.missingValue;
        this.shouldAvoidAllocation = mapToCopy.shouldAvoidAllocation;
        
        keys = mapToCopy.keys.clone();
        values = mapToCopy.values.clone();
    }
    
    /**
     * The value to be used as a null marker in the map.
     *
     * @return value to be used as a null marker in the map.
     */
    public int missingValue()
    {
        return missingValue;
    }
    
    /**
     * Get the load factor beyond which the map will increase size.
     *
     * @return load factor for when the map should increase size.
     */
    public float loadFactor()
    {
        return loadFactor;
    }
    
    /**
     * Get the total capacity for the map to which the load factor will be a fraction of.
     *
     * @return the total capacity for the map.
     */
    public int capacity()
    {
        return values.length;
    }
    
    /**
     * Get the actual threshold which when reached the map will resize.
     * This is a function of the current capacity and load factor.
     *
     * @return the threshold when the map will resize.
     */
    public int resizeThreshold()
    {
        return resizeThreshold;
    }
    
    /**
     * {@inheritDoc}
     */
    public int size()
    {
        return size;
    }
    
    /**
     * {@inheritDoc}
     */
    public boolean isEmpty()
    {
        return 0 == size;
    }
    
    /**
     * {@inheritDoc}
     * Overloaded version of {@link Map#containsKey(Object)} that takes a primitive int key.
     *
     * @param key for indexing the {@link Map}
     * @return true if the key is found otherwise false.
     */
    public boolean containsKey(final Object key)
    {
        final int mask = values.length - 1;
        int index = Hashing.hash(key, mask);
        
        boolean found = false;
        while (missingValue != values[index])
        {
            if (key.equals(keys[index]))
            {
                found = true;
                break;
            }
            
            index = ++index & mask;
        }
        
        return found;
    }
    
    /**
     * {@inheritDoc}
     */
    public boolean containsValue(final Object value)
    {
        return containsValue(((Integer)value).intValue());
    }
    
    public boolean containsValue(final int value)
    {
        if (value == missingValue)
        {
            return false;
        }
        
        boolean found = false;
        for (final int v : values)
        {
            if (value == v)
            {
                found = true;
                break;
            }
        }
        
        return found;
    }
    
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    public Integer get(final Object key)
    {
        return valOrNull(getValue((K)key));
    }
    
    /**
     * Overloaded version of {@link Map#get(Object)} that takes a primitive int key.
     * Due to type erasure have to rename the method
     *
     * @param key for indexing the {@link Map}
     * @return the value if found otherwise missingValue
     */
    public int getValue(final K key)
    {
        final int mask = values.length - 1;
        int index = Hashing.hash(key, mask);
        
        int value;
        while (missingValue != (value = values[index]))
        {
            if (key.equals(keys[index]))
            {
                break;
            }
            
            index = ++index & mask;
        }
        
        return value;
    }
    
    /**
     * Get a value for a given key, or if it does not exist then default the value via a
     * {@link java.util.function.IntFunction} and put it in the map.
     * <p>
     * Primitive specialized version of {@link java.util.Map#computeIfAbsent}.
     *
     * @param key             to search on.
     * @param mappingFunction to provide a value if the get returns missingValue.
     * @return the value if found otherwise the default.
     */
    public int computeIfAbsent(final K key, final ToIntFunction<? super K> mappingFunction)
    {
        int value = getValue(key);
        if (value == missingValue)
        {
            value = mappingFunction.applyAsInt(key);
            if (value != missingValue)
            {
                put(key, value);
            }
        }
        
        return value;
    }
    
    /**
     * {@inheritDoc}
     */
    public Integer put(final K key, final Integer value)
    {
        return valOrNull(put(key, value.intValue()));
    }
    
    /**
     * Overloaded version of {@link Map#put(Object, Object)} that takes a primitive int key.
     *
     * @param key   for indexing the {@link Map}
     * @param value to be inserted in the {@link Map}
     * @return the previous value if found otherwise missingValue
     */
    public int put(final K key, final int value)
    {
        if (value == missingValue)
        {
            throw new IllegalArgumentException("Cannot accept missingValue");
        }
        
        int oldValue = missingValue;
        final int mask = values.length - 1;
        int index = Hashing.hash(key, mask);
        
        while (missingValue != values[index])
        {
            if (key.equals(keys[index]))
            {
                oldValue = values[index];
                break;
            }
            
            index = ++index & mask;
        }
        
        if (missingValue == oldValue)
        {
            ++size;
            keys[index] = key;
        }
        
        values[index] = value;
        
        if (size > resizeThreshold)
        {
            increaseCapacity();
        }
        
        return oldValue;
    }
    
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    public Integer remove(final Object key)
    {
        return valOrNull(removeKey(((K)key)));
    }
    
    /**
     * Overloaded version of {@link Map#remove(Object)} that takes a primitive int key.
     * Due to type erasure have to rename the method
     *
     * @param key for indexing the {@link Map}
     * @return the value if found otherwise missingValue
     */
    public int removeKey(final K key)
    {
        final int mask = values.length - 1;
        int index = Hashing.hash(key, mask);
        
        int value;
        while (missingValue != (value = values[index]))
        {
            if (key.equals(keys[index]))
            {
                keys[index] = null;
                values[index] = missingValue;
                --size;
                
                compactChain(index);
                break;
            }
            
            index = ++index & mask;
        }
        
        return value;
    }
    
    /**
     * {@inheritDoc}
     */
    public void clear()
    {
        if (size > 0)
        {
            Arrays.fill(keys, null);
            Arrays.fill(values, missingValue);
            size = 0;
        }
    }
    
    /**
     * Compact the {@link Map} backing arrays by rehashing with a capacity just larger than current size
     * and giving consideration to the load factor.
     */
    public void compact()
    {
        final int idealCapacity = (int)Math.round(size() * (1.0d / loadFactor));
        rehash(findNextPositivePowerOfTwo(Math.max(MIN_CAPACITY, idealCapacity)));
    }
    
    /**
     * {@inheritDoc}
     */
    public void putAll(final Map<? extends K, ? extends Integer> map)
    {
        for (final Entry<? extends K, ? extends Integer> entry : map.entrySet())
        {
            put(entry.getKey(), entry.getValue());
        }
    }
    
    /**
     * {@inheritDoc}
     */
    public Set<K> keySet()
    {
        if (null == keySet)
        {
            keySet = new KeySet();
        }
        
        return keySet;
    }
    
    /**
     * {@inheritDoc}
     */
    public ValueCollection values()
    {
        if (null == valueCollection)
        {
            valueCollection = new ValueCollection();
        }
        
        return valueCollection;
    }
    
    /**
     * {@inheritDoc}
     */
    public Set<Entry<K, Integer>> entrySet()
    {
        if (null == entrySet)
        {
            entrySet = new EntrySet();
        }
        
        return entrySet;
    }
    
    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        if (isEmpty())
        {
            return "{}";
        }
        
        final EntryIterator entryIterator = new EntryIterator();
        entryIterator.reset();
        
        final StringBuilder sb = new StringBuilder().append('{');
        while (true)
        {
            entryIterator.next();
            sb.append(entryIterator.getKey()).append('=').append(entryIterator.getIntValue());
            if (!entryIterator.hasNext())
            {
                return sb.append('}').toString();
            }
            sb.append(',').append(' ');
        }
    }
    
    /**
     * {@inheritDoc}
     */
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        
        if (!(o instanceof Map))
        {
            return false;
        }
        
        final Map<?, ?> that = (Map<?, ?>)o;
        
        if (size != that.size())
        {
            return false;
        }
        
        for ( int i = 0, length = values.length; i < length; i++)
        {
            final int thisValue = values[i];
            if (missingValue != thisValue)
            {
                final Object thatValueObject = that.get(keys[i]);
                if (!(thatValueObject instanceof Integer))
                {
                    return false;
                }
                
                final int thatValue = (Integer)thatValueObject;
                if (missingValue == thatValue || thisValue != thatValue)
                {
                    return false;
                }
            }
        }
        
        return true;
    }
    
    /**
     * {@inheritDoc}
     */
    public int hashCode()
    {
        int result = 0;
        
        for ( int i = 0, length = values.length; i < length; i++)
        {
            final int value = values[i];
            if (missingValue != value)
            {
                result += (keys[i].hashCode() ^ Integer.hashCode(value));
            }
        }
        
        return result;
    }
    
    /**
     * Primitive specialised version of {@link #replace(Object, Object)}
     *
     * @param key   key with which the specified value is associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with the specified key, or
     * {@code null} if there was no mapping for the key.
     */
    public int replace(final K key, final int value)
    {
        int curValue = getValue(key);
        if (curValue != missingValue)
        {
            curValue = put(key, value);
        }
        
        return curValue;
    }
    
    /**
     * Primitive specialised version of {@link #replace(Object, Object, Object)}
     *
     * @param key      key with which the specified value is associated
     * @param oldValue value expected to be associated with the specified key
     * @param newValue value to be associated with the specified key
     * @return {@code true} if the value was replaced
     */
    public boolean replace(final K key, final int oldValue, final int newValue)
    {
        final int curValue = getValue(key);
        if (curValue == missingValue || curValue != oldValue)
        {
            return false;
        }
        
        put(key, newValue);
        
        return true;
    }
    
    private void increaseCapacity()
    {
        final int newCapacity = values.length << 1;
        if (newCapacity < 0)
        {
            throw new IllegalStateException("Max capacity reached at size=" + size);
        }
        
        rehash(newCapacity);
    }
    
    private void rehash( final int newCapacity)
    {
        final int mask = newCapacity - 1;
        /* */ resizeThreshold = (int)(newCapacity * loadFactor);
        
        @SuppressWarnings("unchecked")
        final K[] tempKeys = (K[])new Object[newCapacity];
        final int[] tempValues = new int[newCapacity];
        Arrays.fill(tempValues, missingValue);
        
        for ( int i = 0, size = values.length; i < size; i++)
        {
            final int value = values[i];
            if (missingValue != value)
            {
                final K key = keys[i];
                int index = Hashing.hash(key, mask);
                while (missingValue != tempValues[index])
                {
                    index = ++index & mask;
                }
                
                tempKeys[index] = key;
                tempValues[index] = value;
            }
        }
        
        keys = tempKeys;
        values = tempValues;
    }
    
    @SuppressWarnings("FinalParameters")
    private void compactChain( int deleteIndex)
    {
        final int mask = values.length - 1;
        int index = deleteIndex;
        
        while (true)
        {
            index = ++index & mask;
            if (missingValue == values[index])
            {
                break;
            }
            
            final int hash = Hashing.hash(keys[index], mask);
            
            if ((index < hash && (hash <= deleteIndex || deleteIndex <= index)) ||
                    (hash <= deleteIndex && deleteIndex <= index))
            {
                keys[deleteIndex] = keys[index];
                values[deleteIndex] = values[index];
                
                keys[index] = null;
                values[index] = missingValue;
                deleteIndex = index;
            }
        }
    }
    
    private Integer valOrNull(final int value)
    {
        return value == missingValue ? null : value;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Internal Sets and Collections
    ///////////////////////////////////////////////////////////////////////////////////////////////
    
    public final class KeySet extends AbstractSet<K> implements Serializable
    {
        private final KeyIterator keyIterator = shouldAvoidAllocation ? new KeyIterator() : null;
        
        /**
         * {@inheritDoc}
         */
        public KeyIterator iterator()
        {
            KeyIterator keyIterator = this.keyIterator;
            if (null == keyIterator)
            {
                keyIterator = new KeyIterator();
            }
            
            keyIterator.reset();
            return keyIterator;
        }
        
        public int size()
        {
            return Object2IntHashMap.this.size();
        }
        
        public boolean contains(final Object o)
        {
            return Object2IntHashMap.this.containsKey(o);
        }
        
        @SuppressWarnings("unchecked")
        public boolean remove(final Object o)
        {
            return missingValue != Object2IntHashMap.this.removeKey((K)o);
        }
        
        public void clear()
        {
            Object2IntHashMap.this.clear();
        }
    }
    
    public final class ValueCollection extends AbstractCollection<Integer> implements Serializable
    {
        private final ValueIterator valueIterator = shouldAvoidAllocation ? new ValueIterator() : null;
        
        /**
         * {@inheritDoc}
         */
        public ValueIterator iterator()
        {
            ValueIterator valueIterator = this.valueIterator;
            if (null == valueIterator)
            {
                valueIterator = new ValueIterator();
            }
            
            valueIterator.reset();
            return valueIterator;
        }
        
        public int size()
        {
            return Object2IntHashMap.this.size();
        }
        
        public boolean contains(final Object o)
        {
            return Object2IntHashMap.this.containsValue(o);
        }
        
        public void clear()
        {
            Object2IntHashMap.this.clear();
        }
    }
    
    public final class EntrySet extends AbstractSet<Entry<K, Integer>> implements Serializable
    {
        private final EntryIterator entryIterator = shouldAvoidAllocation ? new EntryIterator() : null;
        
        /**
         * {@inheritDoc}
         */
        public EntryIterator iterator()
        {
            EntryIterator entryIterator = this.entryIterator;
            if (null == entryIterator)
            {
                entryIterator = new EntryIterator();
            }
            
            entryIterator.reset();
            return entryIterator;
        }
        
        public int size()
        {
            return Object2IntHashMap.this.size();
        }
        
        public void clear()
        {
            Object2IntHashMap.this.clear();
        }
        
        /**
         * {@inheritDoc}
         */
        public boolean contains(final Object o)
        {
            final Entry entry = (Entry)o;
            final Integer value = get(entry.getKey());
            return value != null && value.equals(entry.getValue());
        }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////////////////
    // Iterators
    ///////////////////////////////////////////////////////////////////////////////////////////////
    
    abstract class AbstractIterator<T> implements Iterator<T>, Serializable
    {
        private int posCounter;
        private int stopCounter;
        private int remaining;
        private boolean isPositionValid = false;
        
        protected final int position()
        {
            return posCounter & (values.length - 1);
        }
        
        public boolean hasNext()
        {
            return remaining > 0;
        }
        
        protected final void findNext()
        {
            if (!hasNext())
            {
                throw new NoSuchElementException();
            }
            
            final int[] values = Object2IntHashMap.this.values;
            final int mask = values.length - 1;
            
            for ( int i = posCounter - 1; i >= stopCounter; i--)
            {
                final int index = i & mask;
                if (missingValue != values[index])
                {
                    posCounter = i;
                    isPositionValid = true;
                    --remaining;
                    
                    return;
                }
            }
            
            isPositionValid = false;
            throw new IllegalStateException();
        }
        
        public abstract T next();
        
        public void remove()
        {
            if (isPositionValid)
            {
                final int position = position();
                values[position] = missingValue;
                --size;
                
                compactChain(position);
                
                isPositionValid = false;
            }
            else
            {
                throw new IllegalStateException();
            }
        }
        
        final void reset()
        {
            remaining = Object2IntHashMap.this.size;
            final int[] values = Object2IntHashMap.this.values;
            final int capacity = values.length;
            
            int i = capacity;
            if (missingValue != values[capacity - 1])
            {
                for (i = 0; i < capacity; i++)
                {
                    if (missingValue == values[i])
                    {
                        break;
                    }
                }
            }
            
            stopCounter = i;
            posCounter = i + capacity;
            isPositionValid = false;
        }
    }
    
    public final class ValueIterator extends AbstractIterator<Integer>
    {
        public Integer next()
        {
            return nextInt();
        }
        
        public int nextInt()
        {
            findNext();
            
            return values[position()];
        }
    }
    
    public final class KeyIterator extends AbstractIterator<K>
    {
        @SuppressWarnings("unchecked")
        public K next()
        {
            findNext();
            
            return keys[position()];
        }
    }
    
    @SuppressWarnings("unchecked")
    public final class EntryIterator
        extends AbstractIterator<Entry<K, Integer>>
        implements Entry<K, Integer>
    {
        public Entry<K, Integer> next()
        {
            findNext();
            if (shouldAvoidAllocation)
            {
                return this;
            }
            
            return allocateDuplicateEntry();
        }
        
        private Entry<K, Integer> allocateDuplicateEntry()
        {
            final K k = getKey();
            final int v = getIntValue();
            
            return new Entry<K, Integer>()
            {
                public K getKey()
                {
                    return k;
                }
                
                public Integer getValue()
                {
                    return v;
                }
                
                public Integer setValue(final Integer value)
                {
                    return Object2IntHashMap.this.put(k, value);
                }
                
                public int hashCode()
                {
                    return getKey().hashCode() ^ Integer.hashCode(getIntValue());
                }
                
                public boolean equals(final Object o)
                {
                    if (!(o instanceof Entry))
                    {
                        return false;
                    }
                    
                    final Map.Entry e = (Entry)o;
                    
                    return (e.getKey() != null && e.getValue() != null) &&
                               (e.getKey().equals(k) && e.getValue().equals(v));
                }
                
                public String toString()
                {
                    return k + "=" + v;
                }
            };
        }
        
        public K getKey()
        {
            return keys[position()];
        }
        
        public int getIntValue()
        {
            return values[position()];
        }
        
        public Integer getValue()
        {
            return getIntValue();
        }
        
        public Integer setValue(final Integer value)
        {
            return setValue(value.intValue());
        }
        
        public int setValue(final int value)
        {
            if (value == missingValue)
            {
                throw new IllegalArgumentException("Cannot accept missingValue");
            }
            
            final int pos = position();
            final int oldValue = values[pos];
            values[pos] = value;
            
            return oldValue;
        }
    }
}