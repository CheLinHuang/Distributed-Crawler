public class Hash {

    public static int hashing(String inputString, int numOfBits) {
        int mod = (int) Math.pow(2, numOfBits);
        return (new Integer(inputString.substring(16, 18)) * mod / 15) % mod;
    }

    public static int hashing(String inputString) {
        int mod = 256;
        int hashcode = inputString.split("/")[0].hashCode();
        return (hashcode < 0) ? hashcode % mod + mod : hashcode % mod;
    }

    public static String getServer(int hashValue) {

        int size = Daemon.hashValues.navigableKeySet().size();
        Integer[] keySet = Daemon.hashValues.navigableKeySet().toArray(new Integer[0]);
        if (size == 1) {
            return Daemon.hashValues.get(keySet[0]);
        }

        int min = keySet[0];
        int max = keySet[size - 1];

        if (hashValue > max) {
            return Daemon.hashValues.get(min);
        }
        if (hashValue <= min) {
            return Daemon.hashValues.get(max);
        }
        int targetIndex = -1;
        for (int i = 1; i < size; i++) {
            if (keySet[i] >= hashValue && keySet[i - 1] < hashValue) {
                targetIndex = i;
                break;
            }
        }
        return Daemon.hashValues.get(keySet[targetIndex]);
    }
}
