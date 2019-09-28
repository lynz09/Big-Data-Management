import java.util.Random;
public abstract class CreateData {
    static final String ID = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    static Random r = new Random(30);

    protected final int rowsOfDatasets;
    public CreateData(int rowsOfDatasets) {
        this.rowsOfDatasets = rowsOfDatasets;
    }

    abstract void create();

    String RandomString(int len) {
        StringBuilder s = new StringBuilder(len);
        for (int i = 0; i < len; i++)
            s.append(ID.charAt(r.nextInt(ID.length())));
        return s.toString();
    }

    String[] g = new String[]{"male", "female"};
    String CreateGender(int len ) {
        StringBuilder s = new StringBuilder(len);
        s.append(g[len]);
        return s.toString() ;
    }

}
