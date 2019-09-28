import java.io.PrintWriter;

public class CreateCustomer extends CreateData{
    static final String FILE_NAME = "input/Customers";
    public CreateCustomer(int rowsOfDatasets){
        super(rowsOfDatasets);
    }

    public void create() {
        try {
            int id = 1;
            PrintWriter writer = new PrintWriter(FILE_NAME, "UTF-8");
            while (id <= rowsOfDatasets){
                StringBuilder s = new StringBuilder();
                s.append(id).append(",")   //Id
                        .append(RandomString(r.nextInt(11)+10)).append(",")  //Name
                        .append(r.nextInt(61)+10).append(",")  //Age
                        .append(CreateGender(r.nextInt(2))).append(",")  //Gender
                        .append(r.nextInt(10)+1).append(",")  //Country_code
                        .append(r.nextFloat()*9900+100);  //Salary
                writer.println(s.toString());
                id++;
        }
            writer.close();
        }catch (Exception e){

        }


    }

}

