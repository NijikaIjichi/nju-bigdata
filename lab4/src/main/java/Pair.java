import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class Pair implements Comparable<Pair> {
  private final double first;
  private final Data second;

  public Pair(double f, Data s) {
    first = f;
    second = s;
  }

  public Data getSecond() {
    return second;
  }

  @Override
  public int compareTo(Pair d) {
    return -Double.compare(first, d.first);
  }

  public double getWeight() {
    return Math.exp(-(first * first) / 2);
  }

  public static String mostFrequent(Iterable<Pair> pairs, boolean weighted)
      throws NoSuchElementException {
    HashMap<String, Double> map = new HashMap<>();
    for (Pair p: pairs) {
      String type = p.getSecond().getType();
      map.put(type, map.getOrDefault(type, 0.0) + (weighted ? p.getWeight() : 1.0));
    }
    return map.entrySet().stream().max(Comparator.comparingDouble(Map.Entry::getValue)).
        orElseThrow(NoSuchElementException::new).getKey();
  }
}
