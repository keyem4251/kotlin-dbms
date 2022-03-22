package simpledb.tools

import simpledb.jdbc.embdded.EmbeddedDriver
import java.sql.SQLException

fun main(args: Array<String>) {
    val driver = EmbeddedDriver()
    val url = "jdbc:simpledb:studentdb"

    try {
        val connection = driver.connect(url, null)
        val statement = connection.createStatement()
        val createStudentStatement = "create table STUDENT(SId int, SName varchar(10), MajorId int, GradYear int)"
        statement.executeUpdate(createStudentStatement)
        println("Table STUDENT created.")

        val insertStatement = "insert into STUDENT(SId, SName, MajorId, GradYear) values "
        val studentValues = listOf<String>(
            "(1, 'joe', 10, 2021)",
            "(2, 'any', 20, 2020)",
            "(3, 'max', 10, 2022)",
            "(4, 'sue', 20, 2022)",
            "(5, 'bob', 30, 2020)",
            "(6, 'kim', 20, 2020)",
            "(7, 'art', 30, 2021)",
            "(8, 'pat', 20, 2021)",
            "(9, 'pat', 10, 2019)",
        )
        for (student in studentValues) {
            statement.executeUpdate(insertStatement+student)
        }
        println("STUDENT records inserted.")

        val createDeptStatement = "create table DEPT(DId int, DName varchar(8))"
        statement.executeUpdate(createDeptStatement)
        println("Table DEPT created.")

        val deptInsertStatement = "insert into DEPT(DId, DName) values "
        val deptValues = listOf<String>(
            "(10, 'compsci')",
            "(20, 'math')",
            "(30, 'drama')",
        )
        for (dept in deptValues) {
            statement.executeUpdate(deptInsertStatement+dept)
        }
        println("DEPT records inserted.")

        val createCourseStatement = "create table COURSE(CId int, Title varchar(20), DeptId int)"
        statement.executeUpdate(createCourseStatement)
        println("Table COURSE created.")

        val courseInsertStatement = "insert into COURSE(CId, Title, DeptId) values "
        val courseValues = listOf<String>(
            "(12, 'db systems', 10)",
            "(22, 'compilers', 10)",
            "(32, 'calculus', 20)",
            "(42, 'algebra', 20)",
            "(52, 'acting', 30)",
            "(62, 'elocution', 30)",
        )
        for (course in courseValues) {
            statement.executeUpdate(courseInsertStatement+course)
        }
        println("COURSE records inserted.")

        val createSectionStatement = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)"
        statement.executeUpdate(createSectionStatement)
        println("Table SECTION created.")

        val sectionInsertStatement = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values "
        val sectionValues = listOf<String>(
            "(13, 12, 'turing', 2018)",
            "(23, 12, 'turing', 2019)",
            "(33, 32, 'newton', 2019)",
            "(43, 32, 'einstein', 2017)",
            "(53, 62, 'brando', 2018)",
        )
        for (section in sectionValues) {
            statement.executeUpdate(sectionInsertStatement+section)
        }
        println("SECTION records inserted.")

        val createEnrollStatement = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))"
        statement.executeUpdate(createEnrollStatement)
        println("Table ENROLL created.")

        val enrollInsertStatement = "insert into ENROLL(EId, StudentId, SectionId, Grade) values "
        val enrollValues = listOf<String>(
            "(14, 1, 13, 'A')",
            "(24, 1, 43, 'C')",
            "(34, 2, 43, 'B+')",
            "(44, 4, 53, 'B')",
            "(54, 4, 53, 'A')",
            "(64, 6, 53, 'A')",
        )
        for (enroll in enrollValues) {
            statement.executeUpdate(enrollInsertStatement+enroll)
        }
        println("ENROLL records inserted.")
    } catch (e: SQLException) {
        e.printStackTrace()
    }
}
