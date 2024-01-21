import org.apache.commons.math3.util.Pair;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class GenericPair
    <K extends WritableComparable<? super K>, V extends WritableComparable<? super V>>
    extends Pair<K, V> implements WritableComparable<GenericPair<K, V>> {

  public GenericPair(K k, V v) {
    super(k, v);
  }

  @Override
  public int compareTo(GenericPair<K, V> pairWritable) {
    int r1 = getKey().compareTo(pairWritable.getKey());
    if (r1 != 0) {
      return r1;
    }
    return getValue().compareTo(pairWritable.getValue());
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    getKey().write(dataOutput);
    getValue().write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    getKey().readFields(dataInput);
    getValue().readFields(dataInput);
  }
}