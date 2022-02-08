package heapdb;

public class Application {
    public static void main(String[] args) {
        Schema schema = new Schema();
        schema.addKeyIntType("ID");   // primary key ID int
        schema.addVarCharType("name");
        schema.addVarCharType("dept_name");
        schema.addIntType("salary");
        Tuple[] tuples = new Tuple[]{
                new Tuple(schema, 12121, "Kim", "Elect. Engr.", 65000),
                new Tuple(schema, 19803, "Wisneski", "Comp. Sci.", 46000),
                new Tuple(schema, 24734, "Bruns", "Comp. Sci.", 70000),
                new Tuple(schema, 55552, "Scott", "Math", 80000),
                new Tuple(schema, 12321, "Tao", "Comp. Sci.", 95000)
        };

        Table table = new Table(schema);
        for (Tuple tuple : tuples) {
            table.insert(tuple);
        }
        System.out.println(table);

        // delete
        table.delete(12121);
        System.out.println(table);

        // lookup ok
        System.out.print("lookup 19803: ");
        table.lookup(19803);

        // lookup but not here
        System.out.print("lookup 12121: ");
        table.lookup(12121);

        // lookup from department name
        System.out.println("lookup dept_name=Comp. Sci.: ");
        table.lookup("dept_name", "Comp. Sci.");

        // lookup
        System.out.println("lookup ID=19803: ");
        table.lookup("ID", 19803);

        // lookup
        System.out.println("lookup ID=19802: ");
        table.lookup("ID", 19802);

    }
}
