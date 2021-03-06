package heapdb;

import query.*;

public class Application {
    public static void main(String[] args) {

        Schema instSchema = new Schema();  // primary key ID int
        instSchema.addKeyIntType("ID");
        instSchema.addVarCharType("name");
        instSchema.addVarCharType("dept_name");
        instSchema.addIntType("salary");
        Tuple[] tuples = new Tuple[] {
                new Tuple(instSchema, 22222, "Einstein",    "Physics", 95000),
                new Tuple(instSchema, 12121, "Wu",          "Finance", 90000),
                new Tuple(instSchema, 32343, "El Said" ,    "History", 60000),
                new Tuple(instSchema, 45565, "Katz",        "Comp. Sci.", 75000),
                new Tuple(instSchema, 98345, "Kim",         "Elec. Eng.", 80000),
                new Tuple(instSchema, 10101, "Srinivasan" , "Comp. Sci.", 65000),
                new Tuple(instSchema, 76766, "Crick" ,      "Biology", 72000),
        };
        LSMmemory instTable = new LSMmemory("inst", instSchema);
        for (int i = 0; i < tuples.length; i++) {
            instTable.insert(tuples[i]);
        }

        // department table
        Schema deptSchema = new Schema();
        deptSchema.addKeyVarCharType("dept_name");
        deptSchema.addVarCharType("building");
        deptSchema.addIntType("budget");

        LSMmemory deptTable = new LSMmemory("dept", deptSchema);
        Tuple[] deptTuples = new Tuple[] {
                new Tuple(deptSchema, "Biology",     "Watson",  90000),
                new Tuple(deptSchema, "Comp. Sci.",  "Taylor", 100000),
                new Tuple(deptSchema, "Elec. Eng.",  "Taylor",  85000),
                new Tuple(deptSchema, "Finance",     "Painter",120000),
                new Tuple(deptSchema, "Music",       "Packard", 80000),
                new Tuple(deptSchema, "History",     "Painter", 50000),
                new Tuple(deptSchema, "Physics",     "Watson",  70000),
        };

        for (int i = 0; i < deptTuples.length; i++) {
            deptTable.insert(deptTuples[i]);
        }

        Table joinedTable = SelectQuery.naturalJoin(instTable, deptTable);
        System.out.println(joinedTable);
    }
}
