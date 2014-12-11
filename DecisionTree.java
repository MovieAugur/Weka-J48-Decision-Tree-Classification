import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import weka.classifiers.Evaluation;
import weka.classifiers.meta.FilteredClassifier;
import weka.classifiers.trees.J48;
import weka.core.Instances;
import weka.experiment.InstanceQuery;
import weka.filters.unsupervised.attribute.StringToWordVector;

public class WekaMain {
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://54.86.203.127/TRAIN";

	static final String USER = "bill";
	static final String PASS = "passpass";

	public static void main(String[] args) {
		ResultSet rs = null;
		Connection connection = null;
		Statement stmt = null;
		try {
			InstanceQuery iQuery = new InstanceQuery();
			iQuery.setDatabaseURL(DB_URL);
			iQuery.setUsername(USER);
			iQuery.setPassword(PASS);

			// Select twitter_score, RT_audience_cmt, RT_critic_score,
			// RT_critic_cmt, RT_audience_score, YT_cmt_score, var_coll "
			String selectTrainQuery = "Select twitter_score, RT_audience_cmt, RT_critic_score, RT_critic_cmt, RT_audience_score, YT_cmt_score, starpower, YT_views, YT_likes, YT_dislikes, var_coll "
					+ " from augur_train2"; // assume that only these columns
											// are used to create Instances to
											// train and test data
			String selectTestQuery = "Select twitter_score, RT_audience_cmt, RT_critic_score, RT_critic_cmt, RT_audience_score, YT_cmt_score, starpower, YT_views, YT_likes, YT_dislikes, var_coll "
					+ " from augur_test2";
			String selectMovieNames = "select movie_name from augur_test2";

			iQuery.setQuery(selectTrainQuery);
			Instances train = iQuery.retrieveInstances();
			if (train.classIndex() == -1) {
				train.setClassIndex(train.numAttributes() - 1);
			}
			Instances copyTrain = new Instances(train);
			System.out.println(copyTrain.toSummaryString());
			System.out.println(train.toSummaryString());
			iQuery.setQuery(selectTestQuery);
			Instances test = iQuery.retrieveInstances();
			FilteredClassifier filteredClassifier = new FilteredClassifier();
			filteredClassifier.setFilter(new StringToWordVector());
			filteredClassifier.setClassifier(new J48());
			filteredClassifier.buildClassifier(train);

			Evaluation eval = new Evaluation(train);
			if (test.classIndex() == -1) {
				test.setClassIndex(test.numAttributes() - 1);

			}

			Instances copyTest = new Instances(test);

			System.out.println("Classifier : /////////// " + filteredClassifier);
			String[] movieNames = new String[copyTest.numInstances()];
			Class.forName(JDBC_DRIVER);
			connection = DriverManager.getConnection(DB_URL, USER, PASS);
			stmt = connection.createStatement();
			rs = stmt.executeQuery(selectMovieNames);
			int loopCounter = 0;
			if (rs != null) {
				while (rs.next()) {
					movieNames[loopCounter] = rs.getString("movie_name");
					loopCounter++;
				}
			}
			double classLabel = 0;
			String[] classTypes = new String[copyTest.numInstances()];
			String classType = "";
			for (int i = 0; i < copyTest.numInstances(); i++) {
				classLabel = filteredClassifier.classifyInstance(copyTest
						.instance(i));
				switch ((int) classLabel) {
				case 0:
					classType = "L";
					break;
				case 1:
					classType = "E";
					break;
				case 2:
					classType = "J";
					break;
				case 3:
					classType = "H";
					break;
				case 4:
					classType = "K";
					break;
				case 5:
					classType = "I";
					break;
				case 6:
					classType = "G";
					break;
				case 7:
					classType = "C";
					break;
				case 8:
					classType = "F";
					break;
				case 9:
					classType = "A";
					break;
				case 10:
					classType = "D";
					break;
				
				}
				classTypes[i] = classType;
		}
			updateClass(classTypes, movieNames);

			eval.evaluateModel(filteredClassifier, test);
			System.out.println(eval.toSummaryString("\nResults\n======\n",
					false));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (!rs.isClosed()) {
					rs.close();
				}
				if (!stmt.isClosed()) {
					stmt.close();
				}
				if (!connection.isClosed()) {
					connection.close();
				}
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

	public static void updateClass(String[] classType, String[] movieNames) {
		Connection connection = null;
		PreparedStatement stmt = null;
		if (classType.length != movieNames.length) {
			return;
		}
		try {

			Class.forName(JDBC_DRIVER);
			connection = DriverManager.getConnection(DB_URL, USER, PASS);
			connection.setAutoCommit(false);
			stmt = connection
					.prepareStatement("Update augur_test2 set var_coll = ? where movie_name = ?");
			for (int i = 0; i < classType.length; i++) {
				stmt.setString(1, classType[i]);
				stmt.setString(2, movieNames[i]);
				stmt.addBatch();
			}
			int[] numUpdates = stmt.executeBatch();
			if (numUpdates.length == classType.length) {
				System.out.println("All rows updated");
				connection.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (!stmt.isClosed()) {
					stmt.close();
				}
				if (!connection.isClosed()) {
					connection.close();
				}
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

}
