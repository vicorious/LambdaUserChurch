package co.com.church;

import java.net.URL;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;

import com.amazonaws.util.StringUtils;
import org.json.JSONTokener;
import org.json.JSONArray;
import org.json.JSONObject;


public class Migrate
{

    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_2).build();

    static DynamoDB dynamoDB = new DynamoDB(client);
    static String replyTableName = "usuarios";
    static String fileName = new String("https://importsomosunogo.s3.amazonaws.com/users_export.json");

    /**
     * Main
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        try
        {
            loadDataFromS3Users(replyTableName);
        }
        catch (Exception e) {
            System.err.println("Program failed:");
            System.err.println(e.getMessage());
        }
        System.out.println("Success.");
    }

    /**
     *
     * @param tableName
     */
    private static void loadDataFromS3Users(String tableName)
    {
        try
        {

            Table table = dynamoDB.getTable(tableName);

            System.out.println("Adding data to " + table.getTableName());
            try
            {
                System.out.println("Reading file...");
                URL url = new URL(fileName);
                JSONTokener tokener = new JSONTokener(url.openStream());
                JSONArray jsonArray = new JSONArray(tokener);
                System.out.println("Reading file done!");
                int i = jsonArray.length() + 1;
                for (Object o : jsonArray)
                {
                    JSONObject json = (JSONObject) o;
                    Item item = new Item().withPrimaryKey("id", i)
                            .withString("id", StringUtils.isNullOrEmpty(json.get("id").toString()) ? "null" : json.get("id").toString())
                            .withString("nombre", StringUtils.isNullOrEmpty(json.get("nombre").toString()) ? "null" : json.get("nombre").toString())
                            .withString("apellidos", StringUtils.isNullOrEmpty(json.get("apellidos").toString()) ? "null" : json.get("apellidos").toString())
                            .withString("telefono", StringUtils.isNullOrEmpty(json.get("telefono").toString()) ? "null" : json.get("telefono").toString())
                            .withString("celular", StringUtils.isNullOrEmpty(json.get("celular").toString()) ? "null" : json.get("celular").toString())
                            .withString("email", StringUtils.isNullOrEmpty(json.get("email").toString()) ? "null" : json.get("email").toString())
                            .withString("edad", StringUtils.isNullOrEmpty(json.get("edad").toString()) ? "null" : json.get("edad").toString())
                            .withString("pais", StringUtils.isNullOrEmpty(json.get("pais").toString()) ? "null" : json.get("pais").toString())
                            .withString("ciudad", StringUtils.isNullOrEmpty(json.get("ciudad").toString()) ? "null" : json.get("ciudad").toString())
                            .withString("iglesia", StringUtils.isNullOrEmpty(json.get("iglesia").toString()) ? "null" : json.get("iglesia").toString())
                            .withString("talleres", StringUtils.isNullOrEmpty(json.get("talleres").toString()) ? "null" : json.get("talleres").toString())
                            .withString("otraIglesia", StringUtils.isNullOrEmpty(json.get("otraIglesia").toString()) ? "null" : json.get("otraIglesia").toString())
                            .withString("idioma", StringUtils.isNullOrEmpty(json.get("idioma").toString()) ? "null" : json.get("idioma").toString())
                            .withString("redHombres", StringUtils.isNullOrEmpty(json.get("redHombres").toString()) ? "null" : json.get("redHombres").toString())
                            .withString("redMujeres", StringUtils.isNullOrEmpty(json.get("redMujeres").toString()) ? "null" : json.get("redMujeres").toString())
                            .withString("liderPrincipal", StringUtils.isNullOrEmpty(json.get("liderPrincipal").toString()) ? "null" : json.get("liderPrincipal").toString())
                            .withString("sedeMci", StringUtils.isNullOrEmpty(json.get("sedeMci").toString()) ? "null" : json.get("sedeMci").toString())
                            .withString("red", StringUtils.isNullOrEmpty(json.get("red").toString()) ? "null" : json.get("red").toString())
                            .withString("redEliemerson", StringUtils.isNullOrEmpty(json.get("redEliemerson").toString()) ? "null" : json.get("redEliemerson").toString())
                            .withString("redJohanna", StringUtils.isNullOrEmpty(json.get("redJohanna").toString()) ? "null" : json.get("redJohanna").toString())
                            .withString("redLauGuerra", StringUtils.isNullOrEmpty(json.get("redLauGuerra").toString()) ? "null" : json.get("redLauGuerra").toString())
                            .withString("redSaraCastellanos", StringUtils.isNullOrEmpty(json.get("redSaraCastellanos").toString()) ? "null" : json.get("redSaraCastellanos").toString())
                            .withBoolean("terminosYCondiciones", true)
                            .withBoolean("updated", Boolean.parseBoolean(json.get("updated").toString()))
                            .withString("date_register", StringUtils.isNullOrEmpty(json.get("date_register").toString()) ? "null" : json.get("date_register").toString())
                            .withString("tribu", StringUtils.isNullOrEmpty(json.get("tribu").toString()) ? "null" : json.get("tribu").toString());
                    i++;
                    table.putItem(item);
                    System.out.println("Dato: "+i);
                }
            } catch (Exception e)
            {
                e.printStackTrace();
            }

        }
        catch (Exception e) {
            System.err.println("Failed to create item in " + tableName);
            System.err.println(e.getMessage());

        }
    }

}