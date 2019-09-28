import java.io.PrintWriter;
public class CreateTransaction extends CreateData {
    static final String FILE_NAME = "Transactions";

    public CreateTransaction(int rowsOfDatasets) {
        super(rowsOfDatasets);
    }

    @Override
    public void create(){
        try {
            int id = 1;
            PrintWriter writer = new PrintWriter(FILE_NAME, "UTF-8");
            while (id <=rowsOfDatasets) {
                StringBuilder s = new StringBuilder();
                s.append(id).append(",") // TransID
                        .append(r.nextInt(50000)+1).append(",")     //CustID
                        .append(r.nextFloat()*990+10).append(",")  //TransTotal
                        .append(r.nextInt(10)+1).append(",")  //TransNumItems
                        .append(RandomString(r.nextInt(31)+20));  //TransDesc
                writer.println(s.toString());
                id++;
        }
            writer.close();
    } catch (Exception e){

        }

    }
}