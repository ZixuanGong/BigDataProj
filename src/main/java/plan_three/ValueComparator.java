package plan_three;

import java.util.Comparator;
import java.util.Map;

class ValueComparator implements Comparator<Integer> {

    Map<Integer, Double> base;
    public ValueComparator(Map<Integer, Double> base) {
        this.base = base;
    }

    public int compare(Integer a, Integer b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        }
    }
}
