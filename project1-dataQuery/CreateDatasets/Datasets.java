public class Datasets {
    public static void main(String[] args) {

        CreateData[] l = new CreateData[]{ new CreateCustomer(50000),
                new CreateTransaction(5000000) };

        for (CreateData ll : l) {
            ll.create();
        }
    }
}
