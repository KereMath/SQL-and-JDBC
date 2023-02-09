package ceng.ceng351.foodrecdb;

import java.sql.*;
import java.util.ArrayList;

public class FOODRECDB implements IFOODRECDB{

    private static String user = "e2448462"; // TODO: Your userName
    private static String password = "PIO8GAax7vyoATmA"; //  TODO: Your password
    private static String host = "momcorp.ceng.metu.edu.tr"; // host name
    private static String database = "db2448462"; // TODO: Your database name
    private static int port = 8080; // port

    private static Connection connection = null;
    private Connection con;
    @Override
    public void initialize() {
        String url = "jdbc:mysql://" + this.host + ":" + this.port + "/" + this.database;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            this.con =  DriverManager.getConnection(url, this.user, this.password);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    @Override
    public int createTables() {
        int result;
        int number = 0;
        String queryCreateMenuItemsTable = "create table if not exists MenuItems(" +
                "itemID int," +
                "itemName varchar(40)," +
                "cuisine varchar(20)," +
                "price int," +
                "primary key (itemID))";
        String queryCreateIngredientsTable = "create table if not exists Ingredients(" +
                "ingredientID int," +
                "ingredientName varchar(40)," +
                "primary key (ingredientID))";
        String queryCreateIncludesTable = "create table if not exists Includes(" +
                "itemID int," +
                "ingredientID int," +
                "primary key (itemID, ingredientID),"+
                "foreign key (ingredientID) references Ingredients(ingredientID) on delete cascade on update cascade," +
                "foreign key (itemID) references MenuItems(itemID) on delete cascade on update cascade)" ;
        String queryCreateRatingsTable = "create table if not exists Ratings(" +
                "ratingID int," +
                "itemID int," +
                "rating int," +
                "ratingDate date,"+
                "primary key (ratingID),"+
                "foreign key (itemID) references MenuItems(itemID) on delete cascade on update cascade)";
        String queryCreateDietaryCategoriesTable = "create table if not exists DietaryCategories(" +
                "ingredientID int," +
                "dietaryCategory varchar(20)," +
                "primary key (ingredientID, dietaryCategory),"+
                "foreign key (ingredientID) references Ingredients(ingredientID) on delete cascade on update cascade)" ;
        try {
            Statement statement = this.con.createStatement();
            result = statement.executeUpdate(queryCreateMenuItemsTable);
            System.out.println(result);
            number++;
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Statement statement = this.con.createStatement();
            result = statement.executeUpdate(queryCreateIngredientsTable);
            System.out.println(result);
            number++;
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Statement statement = this.con.createStatement();
            result = statement.executeUpdate(queryCreateIncludesTable);
            System.out.println(result);
            number++;
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Statement statement = this.con.createStatement();
            result = statement.executeUpdate(queryCreateRatingsTable);
            System.out.println(result);
            number++;
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            Statement statement = this.con.createStatement();
            result = statement.executeUpdate(queryCreateDietaryCategoriesTable);
            System.out.println(result);
            number++;
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return number;
    }
    @Override
    public int dropTables() {
        int number_of_tables_dropped = 0;
        int result;
        String query_drop_MenuItems = "DROP TABLE IF EXISTS MenuItems";
        String query_drop_Ingredients = "DROP TABLE IF EXISTS Ingredients";
        String query_drop_Includes = "DROP TABLE IF EXISTS Includes";
        String query_drop_Ratings = "DROP TABLE IF EXISTS Ratings";
        String query_drop_DietaryCategories = "DROP TABLE IF EXISTS DietaryCategories";
        try{
            Statement statement = con.createStatement();
            result = statement.executeUpdate(query_drop_Includes);
            number_of_tables_dropped++;
            System.out.println(result);
            statement.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }

        // User Table
        try{
            Statement statement = con.createStatement();
            result = statement.executeUpdate(query_drop_Ratings);
            number_of_tables_dropped++;
            System.out.println(result);
            statement.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }
        try{
            Statement statement = con.createStatement();
            result = statement.executeUpdate(query_drop_DietaryCategories);
            number_of_tables_dropped++;
            System.out.println(result);
            statement.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }
        try{
            Statement statement = con.createStatement();
            result = statement.executeUpdate(query_drop_Ingredients);
            number_of_tables_dropped++;
            System.out.println(result);
            statement.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }
        // close the db connection
        try{
            Statement statement = con.createStatement();
            result = statement.executeUpdate(query_drop_MenuItems);
            number_of_tables_dropped++;
            System.out.println(result);
            statement.close();
        }catch (SQLException e) {
            e.printStackTrace();
        }

        return number_of_tables_dropped;
    }

    @Override
    public int insertMenuItems(MenuItem[] items) {
        int number1 = 0;
        String insert_query = "INSERT INTO MenuItems(" +
                "itemID, " +
                "itemName, " +
                "cuisine, " +
                "price) VALUES (?, ?, ?, ?);";
        for(int i = 0; i < items.length; i++){
            MenuItem target = items[i];
            try{
                PreparedStatement prep_statement = con.prepareStatement(insert_query);
                prep_statement.setInt(1, target.getItemID());
                prep_statement.setString(2, target.getItemName());
                prep_statement.setString(3, target.getCuisine());
                prep_statement.setInt(4, target.getPrice());
                prep_statement.executeUpdate();
                number1++;
                prep_statement.close();
            }catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return number1;
    }
    @Override
    public int insertIngredients(Ingredient[] ingredients) {
        int number2 = 0;
        String insert_query = "INSERT INTO Ingredients(" +
                "ingredientID, " +
                "ingredientName) VALUES (?, ?);";
        for(int i = 0; i < ingredients.length; i++){
            Ingredient target = ingredients[i];
            try{
                PreparedStatement prep_statement = con.prepareStatement(insert_query);
                prep_statement.setInt(1, target.getIngredientID());
                prep_statement.setString(2, target.getIngredientName());
                prep_statement.executeUpdate();
                number2++;
                prep_statement.close();
            }catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return number2;
    }
    @Override
    public int insertIncludes(Includes[] includes) {
        int number3 = 0;
        String insert_query = "INSERT INTO Includes(" +
                "itemID, " +
                "ingredientID) VALUES (?, ?);";
        for (Includes target : includes) {
            try {
                PreparedStatement prep_statement = con.prepareStatement(insert_query);
                prep_statement.setInt(1, target.getItemID());
                prep_statement.setInt(2, target.getIngredientID());
                prep_statement.executeUpdate();
                number3++;
                prep_statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return number3;
    }
    @Override
    public int insertDietaryCategories(DietaryCategory[] categories) {
        int number4 = 0;
        String insert_query = "INSERT INTO DietaryCategories(" +
                "ingredientID, " +
                "dietaryCategory) VALUES (?, ?);";
        for (DietaryCategory target : categories) {
            try {
                PreparedStatement prep_statement = con.prepareStatement(insert_query);
                prep_statement.setInt(1, target.getIngredientID());
                prep_statement.setString(2, target.getDietaryCategory());
                prep_statement.executeUpdate();
                number4++;
                prep_statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return number4;
    }
    @Override
    public int insertRatings(Rating[] ratings) {
        int number5 = 0;
        String insert_query = "INSERT INTO Ratings(" +
                "ratingID, " +
                "itemID, " +
                "rating, " +
                "ratingDate) VALUES (?, ?, ?, ?);";
        for (Rating target : ratings) {
            try {
                PreparedStatement prep_statement = con.prepareStatement(insert_query);
                prep_statement.setInt(1, target.getRatingID());
                prep_statement.setInt(2, target.getItemID());
                prep_statement.setInt(3, target.getRating());
                prep_statement.setString(4, target.getRatingDate());
                prep_statement.executeUpdate();
                number5++;
                prep_statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return number5;
    }
    @Override
    public MenuItem[] getMenuItemsWithGivenIngredient(String name) {
        String q = "select distinct M.itemID, M.itemName, M.cuisine, M.price " +
                "from Ingredients I, Includes G, MenuItems M  where I.ingredientName="+"'"+name+"'"+
                " and G.ingredientID=I.ingredientID and M.itemID=G.itemID " +
                "order by M.itemID ASC;";
        ResultSet rs;
        ArrayList<MenuItem> reslist = new ArrayList<>();
        try {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(q);
            while (rs.next()) {
                Integer itemID = rs.getInt("itemID");
                String itemName = rs.getString("itemName");
                String cuisine = rs.getString("cuisine");
                Integer price = rs.getInt("price");
                MenuItem obj = new MenuItem(itemID,itemName,cuisine,price);
                reslist.add(obj);
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        MenuItem[] resarray = new MenuItem[reslist.size()];
        return reslist.toArray(resarray);
    }
    @Override
    public MenuItem[] getMenuItemsWithoutAnyIngredient() {
        String q = "SELECT DISTINCT M.itemID, M.itemName, M.cuisine, M.price\n" +
                "FROM MenuItems M\n" +
                "WHERE M.itemID NOT IN (SELECT M.itemID FROM MenuItems M, Ingredients Ig, Includes Ic" +
                " WHERE M.itemID = Ic.itemID AND Ig.ingredientID = Ic.ingredientID)\n" +
                "ORDER BY M.itemID ASC;" ;
        ResultSet rs;
        ArrayList<MenuItem> reslist = new ArrayList<>();
        try {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(q);
            while (rs.next()) {
                Integer itemID = rs.getInt("itemID");
                String itemName = rs.getString("itemName");
                String cuisine = rs.getString("cuisine");
                Integer price = rs.getInt("price");
                MenuItem obj = new MenuItem(itemID,itemName,cuisine,price);
                reslist.add(obj);
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        MenuItem[] resarray = new MenuItem[reslist.size()];
        return reslist.toArray(resarray);
    }
    @Override
    public Ingredient[] getNotIncludedIngredients() {
        String q = "SELECT DISTINCT Ig.ingredientID, Ig.ingredientName\n" +
                "FROM Ingredients Ig\n" +
                "WHERE Ig.ingredientID NOT IN (SELECT Ig.ingredientID FROM MenuItems M, Ingredients Ig, Includes Ic" +
                " WHERE M.itemID = Ic.itemID AND Ig.ingredientID = Ic.ingredientID)\n" +
                "ORDER BY Ig.ingredientID ASC;" ;
        ResultSet rs;
        ArrayList<Ingredient> reslist = new ArrayList<>();
        try {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(q);
            while (rs.next()) {
                Integer ingredientID = rs.getInt("ingredientID");
                String ingredientName = rs.getString("ingredientName");
                Ingredient obj1 = new Ingredient(ingredientID,ingredientName);
                reslist.add(obj1);
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } Ingredient[] resarray = new Ingredient[reslist.size()];
        return reslist.toArray(resarray);
    }
    @Override
    public MenuItem getMenuItemWithMostIngredients() {
        MenuItem result = null;
        String q = "select M.itemID, M.itemName, M.cuisine, M.price " +
                "from MenuItems M, Includes I " +
                "where M.itemID = I.itemID " +
                "group by M.itemID " +
                "having count(*) = (select max(ingcounter) " +
                "from (select count(*) as ingcounter " +
                "from MenuItems M1, Includes I1 " +
                "where M1.itemID = I1.itemID " +
                "group by M1.itemID) need);";
        ResultSet rs;
        try {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(q);
            while (rs.next()) {
                Integer itemID = rs.getInt("itemID");
                String itemName = rs.getString("itemName");
                String cuisine = rs.getString("cuisine");
                Integer price = rs.getInt("price");
                MenuItem menuItem = new MenuItem(itemID, itemName, cuisine, price);
                result = menuItem;
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }
    @Override
    public QueryResult.MenuItemAverageRatingResult[] getMenuItemsWithAvgRatings() {
        ResultSet rs;
        ArrayList<QueryResult.MenuItemAverageRatingResult> reslist = new ArrayList<>();
        String q = "SELECT DISTINCT M.itemID, M.itemName, avg(R.rating) as avgr \n" +
                "FROM MenuItems M NATURAL LEFT OUTER JOIN Ratings R\n" +
                "Group by M.itemID \n " +
                "ORDER BY avgr DESC;" ;
        try {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(q);
            while (rs.next()) {
                String itemID = String.valueOf(rs.getInt("itemID"));
                String itemName = String.valueOf(rs.getString("itemName"));
                String avgr = rs.getString("avgr");
                QueryResult.MenuItemAverageRatingResult obj = new QueryResult.MenuItemAverageRatingResult(itemID, itemName, avgr);
                reslist.add(obj);
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        QueryResult.MenuItemAverageRatingResult[] resarray = new QueryResult.MenuItemAverageRatingResult[reslist.size()];
        return reslist.toArray(resarray);
    }
    @Override
    public MenuItem[] getMenuItemsForDietaryCategory(String category) {
        String q = "SELECT DISTINCT M.itemID, M.itemName, M.cuisine, M.price\n" +
                "FROM MenuItems M, Includes Ic, Ingredients I\n" +
                "where I.ingredientID=Ic.ingredientID and M.itemID=Ic.itemID \n" +
                "group by M.itemID, M.itemName, M.cuisine, M.price \n" +
                "having count(*) = " +
                "(select count(*) from Ingredients I1, Includes Ic1, DietaryCategories D \n" +
                "where I1.ingredientID=D.ingredientID and M.itemID=Ic1.itemID and Ic1.ingredientID=I1.ingredientID and D.dietaryCategory="+"'"+category+"'" +
                ")\n" +
                "ORDER BY M.itemID ASC;" ;
        ResultSet rs;
        ArrayList<MenuItem> reslist = new ArrayList<>();
        try {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(q);
            while (rs.next()) {
                Integer itemID = rs.getInt("itemID");
                String itemName = rs.getString("itemName");
                String cuisine = rs.getString("cuisine");
                Integer price = rs.getInt("price");
                MenuItem obj = new MenuItem(itemID,itemName,cuisine,price);
                reslist.add(obj);
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        MenuItem[] resarray = new MenuItem[reslist.size()];
        return reslist.toArray(resarray);
    }
    @Override
    public Ingredient getMostUsedIngredient() {
        Ingredient result = null;
        String q = " select distinct I.ingredientID, I.ingredientName " +
                " from Includes Ic, Ingredients I " +
                " where Ic.ingredientID=I.ingredientID " +
                " group by I.ingredientID, I.ingredientName " +
                " having count(*) = (select max(ingcounter) " +
                " from (select count(*) as ingcounter " +
                " from Includes Ic1, Ingredients I1 " +
                " where Ic1.ingredientID=I1.ingredientID " +
                " group by I1.ingredientID, I1.ingredientName ) tbl);";
        ResultSet rs;
        try {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(q);
            while (rs.next()) {
                Integer ingredientID = rs.getInt("ingredientID");
                String ingredientName = rs.getString("ingredientName");
                result = new Ingredient(ingredientID, ingredientName);
                break;
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;

    }
    @Override
    public QueryResult.CuisineWithAverageResult[] getCuisinesWithAvgRating() {
        ResultSet rs;
        ArrayList<QueryResult.CuisineWithAverageResult> reslist = new ArrayList<>();
        String q = "Select distinct M.cuisine, AVG(R.rating) from MenuItems M NATURAL LEFT OUTER JOIN Ratings R group by M.cuisine order by AVG(R.rating) DESC;" ;
        try {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(q);
            while (rs.next()) {
                String cuisine = rs.getString("cuisine");
                String avgr = rs.getString("AVG(R.rating)");
                QueryResult.CuisineWithAverageResult obj = new QueryResult.CuisineWithAverageResult(cuisine,avgr);
                reslist.add(obj);
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        QueryResult.CuisineWithAverageResult[] resarray = new QueryResult.CuisineWithAverageResult[reslist.size()];
        return reslist.toArray(resarray);
    }
    @Override
    public QueryResult.CuisineWithAverageResult[] getCuisinesWithAvgIngredientCount() {
        ResultSet rs;
        ArrayList<QueryResult.CuisineWithAverageResult> reslist = new ArrayList<>();
        String q = "Select distinct cs, AVG(countids) " +
                "FROM(SELECT DISTINCT M.itemID, M.Cuisine as cs, Count(I.itemID) as countids " +
                "From MenuItems M NATURAL LEFT OUTER JOIN Includes I GROUP BY M.itemID,M.cuisine) tbl GROUP BY cs order by avg(countids) DESC;\n;" ;
        try {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(q);
            while (rs.next()) {
                String cs = rs.getString("cs");
                String countids = rs.getString("AVG(countids)");
                QueryResult.CuisineWithAverageResult obj = new QueryResult.CuisineWithAverageResult(cs,countids);
                reslist.add(obj);
            }
            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        QueryResult.CuisineWithAverageResult[] resarray = new QueryResult.CuisineWithAverageResult[reslist.size()];
        return reslist.toArray(resarray);
    }
    @Override
    public int increasePrice(String ingredientName, String increaseAmount) {
        int rows=0;
        String q = "update MenuItems M, Ingredients I, Includes Ic " +
                "set M.price = M.price + " +"'"+increaseAmount+"'" +
                "where M.itemID=Ic.itemID and Ic.ingredientID=I.ingredientID and + I.ingredientName=" +"'"+ingredientName+"'";
        try {
            Statement stat = this.con.createStatement();
            rows = stat.executeUpdate(q);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rows;
    }
    @Override
    public Rating[] deleteOlderRatings(String date) {
        String q = "SELECT DISTINCT R.ratingID, R.itemID, R.rating, R.ratingDate\n" +
                "FROM Ratings R\n" +
                "where R.ratingDate <"+"'"+date+"'\n" +
                "ORDER BY R.ratingID ASC;";
        String q1 = "DELETE FROM Ratings  where ratingDate <"+"'"+date+"'";
        ResultSet rs;
        ArrayList<Rating> reslist = new ArrayList<>();
        try {
            Statement st = this.con.createStatement();
            rs = st.executeQuery(q);
            while (rs.next()) {
                Integer ratingID = rs.getInt("ratingID");
                Integer itemID = rs.getInt("itemID");
                Integer rating = rs.getInt("rating");
                String ratingDate = rs.getString("ratingDate");
                Rating obj = new Rating(ratingID,itemID,rating,ratingDate);
                reslist.add(obj);
            }
            st.executeUpdate(q1);

            st.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        Rating[] resarray = new Rating[reslist.size()];
        return reslist.toArray(resarray);
    }
}
