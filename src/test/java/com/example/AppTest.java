package com.example;

import org.junit.jupiter.api.Test;

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.*;
import org.hsqldb.jdbc.JDBCDataSource;

import java.sql.*;

class AppTest {

    static String ddl = """
            CREATE TABLE "houses" (
                "id" INTEGER NOT NULL,
                "address" VARCHAR(255) NOT NULL,
            );
            CREATE TABLE "users" (
                "id" INTEGER NOT NULL,
                "name" VARCHAR(255) NOT NULL,
                "house_id" INTEGER NOT NULL
            );
            CREATE TABLE "todos" (
                "id" INTEGER NOT NULL,
                "description" VARCHAR(255) NOT NULL,
                "user_id" INTEGER NOT NULL
            );
            """;

    static String dml = """
            INSERT INTO "houses" VALUES (1, '123 Main St');
            INSERT INTO "houses" VALUES (2, '456 Ocean Ave');

            INSERT INTO "users" VALUES (1, 'Alice', 1);
            INSERT INTO "users" VALUES (2, 'Bob', 1);
            INSERT INTO "users" VALUES (3, 'Carol', 2);

            INSERT INTO "todos" VALUES (1, 'Buy milk', 1);
            INSERT INTO "todos" VALUES (2, 'Buy eggs', 1);
            INSERT INTO "todos" VALUES (3, 'Buy bread', 2);
            INSERT INTO "todos" VALUES (4, 'Vacuum', 3);
            """;

    static String query = """
                select
                "g0"."id" "id",
                "g0"."address" "address",
                (
                select json_arrayagg(json_object(
                    key 'id' value "g1"."id",
                    key 'todos' value ((
                    select json_arrayagg(json_object(
                        key 'id' value "g2"."id",
                        key 'description' value "g2"."description",
                    ))
                    from (
                        select * from "todos"
                        where "g1"."id" = "user_id"
                        order by "id"
                    ) "g2"
                    ))
                ))
                from (
                    select * from "users"
                    where "g0"."id" = "house_id"
                    order by "id"
                ) "g1"
                ) "users"
            from "houses" "g0"
            order by "g0"."id"
                """;

    @Test
    public void brokenQueryTest() throws Exception {

        JDBCDataSource hsqldb = new org.hsqldb.jdbc.JDBCDataSource();
        hsqldb.setDatabase("jdbc:hsqldb:mem:house_user_todo");

        try (Connection connection = hsqldb.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(ddl);
                statement.execute(dml);
            }
        }

        SchemaPlus rootSchema = Frameworks.createRootSchema(false);
        rootSchema.add("house_user_todo", JdbcSchema.create(rootSchema, "house_user_todo", hsqldb, null, null));

        FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.config().withCaseSensitive(false))
                .defaultSchema(rootSchema.getSubSchema("house_user_todo"))
                .build();

        Planner planner = Frameworks.getPlanner(config);
        
        /**
         * org.apache.calcite.sql.parser.SqlParseException: Query expression encountered in illegal context
         *  at org.apache.calcite.sql.parser.impl.SqlParserImpl.convertException(SqlParserImpl.java:389)
         *  at org.apache.calcite.sql.parser.impl.SqlParserImpl.normalizeException(SqlParserImpl.java:153)
         *  at org.apache.calcite.sql.parser.SqlParser.handleException(SqlParser.java:145)
         *  at org.apache.calcite.sql.parser.SqlParser.parseQuery(SqlParser.java:160)
         *  at org.apache.calcite.sql.parser.SqlParser.parseStmt(SqlParser.java:185)
         *  at org.apache.calcite.prepare.PlannerImpl.parse(PlannerImpl.java:214)
         *  at org.apache.calcite.tools.Planner.parse(Planner.java:50)
         */
        SqlNode sqlNode = planner.parse(query);
        SqlNode validated = planner.validate(sqlNode);
        RelNode relNode = planner.rel(validated).project();

        Class.forName(Driver.class.getName());
        Connection connection = DriverManager.getConnection(Driver.CONNECT_STRING_PREFIX);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        RelRunner runner = calciteConnection.unwrap(RelRunner.class);

        try (PreparedStatement ps = runner.prepareStatement(relNode)) {
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                ResultSetMetaData md = rs.getMetaData();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    System.out.print(md.getColumnName(i) + ": ");
                    System.out.println(rs.getObject(i));
                }
                System.out.println();
            }
        }
    }

}
