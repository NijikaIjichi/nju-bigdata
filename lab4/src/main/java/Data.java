public class Data {
  private final double[] data;
  private final String type;

  public Data(String[] d) {
    data = new double[4];
    for (int i = 0; i < 4; ++i) {
      data[i] = Double.parseDouble(d[i]);
    }
    if (d.length > 4) {
      type = d[4];
    } else {
      type = null;
    }
  }

  public String getType() {
    return type;
  }

  public double distance(Data d) {
    double dis = 0;
    for (int i = 0; i < 4; ++i) {
      dis += Math.pow(data[i] - d.data[i], 2);
    }
    return dis;
  }
}
