package pure.spark.labs;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import many_labs.Member;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import scala.Function0;
import scala.collection.immutable.Seq;
import scala.reflect.ClassTag;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.util.List.of;
import static org.apache.spark.sql.functions.*;

/**
 * @author Evgeny Borisov
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class CSVFilesConfTest {

    @Autowired
    Dataset<Row> membersDf;
    @Autowired
    Dataset<Row> facilitiesDf;
    @Autowired
    Dataset<Row> bookingsDf;
    @Autowired
    private SparkSession spark;

    @Test
    public void simpleInnerJoin() {
        membersDf.join(bookingsDf,membersDf.col("memid").equalTo(bookingsDf.col("memid")));
    }

    @Test
    public void test1Or5() {
        facilitiesDf.filter(col("facid").equalTo(1).or(col("facid").equalTo(5))).show();
    }

    @Test
    public void convertDfToDs() {
        Dataset<Member> memberDataset = membersDf.as(Encoders.bean(Member.class));
        Member member = memberDataset.reduce((ReduceFunction<Member>) (member1, member2) -> member1);
        System.out.println("member = " + member);
    }

    @Test
    public void retrieveTheStartTimesOfMembersBookings() {
        Dataset<Row> davidDf = membersDf.filter(lower(col("firstname")).equalTo("david").and(lower(col("surname")).equalTo("farrell")));
        davidDf.join(bookingsDf, bookingsDf.col("memid").equalTo(membersDf.col("memid"))).select("starttime").show(40);
    }

    @Test
    public void startTimesOfBookingsFoTennisCourts() {
        Dataset<Row> tennisDf = facilitiesDf.filter(lower(col("name")).like("tennis court %"));
        Dataset<Row> starttimeDf = bookingsDf.filter(to_date(col("starttime")).equalTo("2012-09-21"));
        tennisDf.join(starttimeDf, bookingsDf.col("facid").equalTo(facilitiesDf.col("facid"))).select(col("starttime").as("start"), col("name")).orderBy("start").show(false);
    }



    @Test
    public void membersWhoHaveRecommendedAnotherMember() {
        Dataset<Row> recommendedby = membersDf.select(col("recommendedby")).distinct();
        recommendedby.join(membersDf.drop("recommendedby"), recommendedby.col("recommendedby").equalTo(membersDf.col("memid"))).select("surname", "firstname")
                .orderBy("surname", "firstname")
                .show();
    }







    @Test
    public void membersWhoHaveRecommendedAnotherMemberWithRecommender() {
        Dataset<Row> recommendators = membersDf.select(col("memid"), col("surname").as("recSurname"), col("firstname").as("recFname"));
        membersDf.join(recommendators, membersDf.col("recommendedby").equalTo(recommendators.col("memid")))
                .select("firstname", "surname", "recFname", "recSurname").orderBy("surname", "firstname").show();

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Person {
        private Integer id;
        private String name;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Dog {
        private int dogid;
        private int personid;
        private String name;
    }

    @Test
    public void testJoins() {
        Dataset<Row> personDF = spark.createDataset(List.of(new Person(1, "Jack"), new Person(2, "John"),new Person(3,"Bill")), Encoders.bean(Person.class)).toDF();
        Dataset<Row> dogDF = spark.createDataset(of(new Dog(10,1, "Pudel"), new Dog(20,1, "Ovcharka"),new Dog(30,2,"Kolli"),new Dog(40,123,"Dvornyaga")), Encoders.bean(Dog.class)).toDF();
        System.out.println("Simple join - inner");
        personDF.join(dogDF,dogDF.col("personid").equalTo(personDF.col("id"))).show();
        System.out.println("left outer");
        personDF.join(dogDF,dogDF.col("personid").equalTo(personDF.col("id")),"left_outer").show();
        System.out.println("right outer");
        personDF.join(dogDF,dogDF.col("personid").equalTo(personDF.col("id")),"right_outer").show();
        System.out.println("full join");
        personDF.join(dogDF,dogDF.col("personid").equalTo(personDF.col("id")),"full_outer").show();
        System.out.println("left semi");
        personDF.join(dogDF,dogDF.col("personid").equalTo(personDF.col("id")),"left_semi").show();

        System.out.println("left anti");
        personDF.join(dogDF,dogDF.col("personid").equalTo(personDF.col("id")),"left_anti").show();
    }

    @Test
    public void allMembersWhoHaveUsedTennisCourt() {
        Dataset<Row> members = membersDf.withColumn("member",concat(col("firstname"),(lit(" ")),(col("surname")))).select("memid", "member");
        members.show();
        Dataset<Row> facilities = facilitiesDf.select(col("facid"), col("name").as("facility"))
                .filter(col("facility").like("Tennis Court%"))
                .join(bookingsDf,facilitiesDf.col("facid").equalTo(bookingsDf.col("facid")));
        members.join(facilities, members.col("memid").equalTo(facilities.col("memid")))
                .select("member","facility")
                .distinct()
                .orderBy("member","facility").show();
    }


    @Test
    public void costlyBookings() {
        bookingsDf.filter(to_date(col("starttime")).equalTo("2012-09-14"))
                .join(facilitiesDf.select("facid","name","membercost","guestcost"),facilitiesDf.col("facid").equalTo(bookingsDf.col("facid")))
                .withColumn("cost",when(bookingsDf.col("memid").notEqual(0),
                        facilitiesDf.col("membercost").multiply(bookingsDf.col("slots")))
                        .otherwise(col("guestcost").multiply(bookingsDf.col("slots"))))
                .join(membersDf.withColumn("member",concat(col("surname"),lit(" "),col("firstname"))),membersDf.col("memid").equalTo(bookingsDf.col("memid")))
                .select(col("member"),col("name").as("facility"),col("cost"))
                .orderBy(col("cost").desc())
                .show();





    }


    @Test
    public void testUdf() {
        membersDf.withColumn("nickName", callUDF(NickNameCalculatorUdf.class.getName(), col("memid"))).select("memid","surname","nickName").show(false);
    }
}










