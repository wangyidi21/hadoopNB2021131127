package org.example;//package org.example;




import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;


public class NB {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		predictAll();
	}

	/**
	 * 预测模型采用多项式模型
//	 */
//	private static final String modelFilePath = "D:\\Study\\Hadoop\\test_\\output.txt";
//	private static final String testDataFilePath = "D:\\Study\\Hadoop\\test_\\test.txt";
    private static String modelFilePath = "E:\\study\\hadoop\\output\\2021131127_模型.txt";
    private static String testDataFilePath = "E:\\study\\hadoop\\doc\\test.txt";
	private static final String outPath="2021131127_预测结果.txt";
	public static HashMap<String, Integer> parameters = null; // 情感标签集
	public static double Nd = 0.;// 文件中的总记录数
	public static HashMap<String, Integer> allFeatures = null;// 整个训练样本的键值对
	public static HashMap<String, Double> labelFeatures = null;// 某一类别下所有词出现的总数
	public static HashSet<String> V = null;// 总训练样本的不重复单词

	/**
	 * 对训练数据进行二次处理，得到多项式模型
	 */
	public static void loadModel(String modelFile) throws Exception {
		if (parameters != null && allFeatures != null) {
			return;
		}
		parameters = new HashMap<String, Integer>();// 情感标签集
		allFeatures = new HashMap<String, Integer>();// 全部属性对
		labelFeatures = new HashMap<String, Double>();// 某一类别下所有词出现的总数
		V = new HashSet<String>();
		BufferedReader br = new BufferedReader(new FileReader(modelFile));
		String line = null;
		while ((line = br.readLine()) != null) {
			String feature = line.substring(0, line.indexOf("\t"));
			Integer count = Integer.parseInt(line.substring(line.indexOf("\t") + 1));
			if (feature.contains("_")) {
				allFeatures.put(feature, count);
				String label = feature.substring(0, feature.indexOf("_"));
				if (labelFeatures.containsKey(label)) {
					labelFeatures.put(label, labelFeatures.get(label) + count);
				} else {
					labelFeatures.put(label, (double) count);
				}
				String word = feature.substring(feature.indexOf("_") + 1);
				if (!V.contains(word)) {
					V.add(word);
				}
			} else {
				parameters.put(feature, count);
				Nd += count;
			}
		}
		br.close();
	}

	/**
	 * 计算条件概率
	 */
	public static String predict(String sentence, String modelFile) throws Exception {
		loadModel(modelFile);
		String predLabel = null;
		double maxValue = Double.NEGATIVE_INFINITY;// 最大类概率（默认值为负无穷小）
		String[] words = sentence.split(" ");
		Set<String> labelSet = parameters.keySet(); // 获得标签集
		for (String label : labelSet) {
			double tempValue = Math.log(parameters.get(label) / Nd);// 先验概率
			/**
			 * 先验概率P(c)= 类c下单词总数/整个训练样本的单词总数 parameters .get(label):类别c对应的文档在训练数据集中的计数
			 * Nd:整个训练样本的单词总数
			 */
			for (String word : words) {
				String lf = label + "_" + word;
				// 计算最大似然概率
				if (allFeatures.containsKey(lf)) {
					tempValue += Math.log((double) (allFeatures.get(lf) + 1) / (labelFeatures.get(label) + V.size()));
					/**
					 * 多项式原理 类条件概率P(tk|c)=(类c下单词tk在各个文档中出现过的次数之和+1)/(类c下单词总数+|V|)
					 * allFeatures.get(lf)：类别c与词语 w共同出现的次数 labelFeatures.get(label) +
					 * V.size()：类别c下属性总数+该训练文本中词语总数 Laplace Smoothing处理未出现在训练集中的数据 +1
					 */
				} else {
					tempValue += Math.log((double) (1 / (labelFeatures.get(label) + V.size())));
				}
			}
			if (tempValue > maxValue) {
				maxValue = tempValue;
				predLabel = label;
			}
		}
		return predLabel;
	}

	public static void predictAll() {
		double accuracy = 0.;
		int amount = 0;
		try {
			File file = new File(outPath);
			file.createNewFile();
			BufferedWriter out = new BufferedWriter(new FileWriter(file));
			List<String> testData = Files.readAllLines(Paths.get(testDataFilePath));
			for (String instance : testData) {
				String gold = instance.substring(0, instance.indexOf("："));
				String sentence = instance.substring(instance.indexOf("：") + 1);
				String prediction = predict(sentence, modelFilePath);
				System.out.println("Gold='" + gold + "'\tPrediction='" + prediction + "'");
				if (gold.equals(prediction)) {
					accuracy += 1;
				}
				amount += 1;
				out.write(amount + "\t" + prediction+"\n");
			}
			out.flush();
			out.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("Accuracy = " + accuracy / amount);
	}
}
//
//
//
// java.io.*;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//
//public class NB {
//
//    public static void main(String[] args) {
//        // TODO Auto-generated method stub
//        predictAll();
//    }
//
//    /**
//     * 预测模型采用多项式模型
//     */
//    private static String modelFilePath = "E:\\study\\hadoop\\output\\2021131127_模型.txt";
//    private static String testDataFilePath = "E:\\study\\hadoop\\doc\\test.txt";
//    public static HashMap<String, Integer> parameters = null; // 情感标签集
//    public static double Nd = 0.;// 文件中的总记录数
//    public static HashMap<String, Integer> allFeatures = null;// 整个训练样本的键值对
//    public static HashMap<String, Double> labelFeatures = null;// 某一类别下所有词出现的总数
//    public static HashSet<String> V = null;// 总训练样本的不重复单词
//
//    /**
//     * 对训练数据进行二次处理，得到多项式模型
//     */
//    public static void loadModel(String modelFile) throws Exception {
//        if (parameters != null && allFeatures != null) {
//            return;
//        }
//        parameters = new HashMap<String, Integer>();// 情感标签集
//        allFeatures = new HashMap<String, Integer>();// 全部属性对
//        labelFeatures = new HashMap<String, Double>();// 某一类别下所有词出现的总数
//        V = new HashSet<String>();
//        BufferedReader br = new BufferedReader(new FileReader(modelFile));
//        String line = null;
//        while ((line = br.readLine()) != null) {
//            String feature = line.substring(0, line.indexOf("\t"));
//            Integer count = Integer.parseInt(line.substring(line.indexOf("\t") + 1));
//            if (feature.contains("_")) {
//                allFeatures.put(feature, count);
//                String label = feature.substring(0, feature.indexOf("_"));
//                if (labelFeatures.containsKey(label)) {
//                    labelFeatures.put(label, labelFeatures.get(label) + count);
//                } else {
//                    labelFeatures.put(label, (double) count);
//                }
//                String word = feature.substring(feature.indexOf("_") + 1);
//                if (!V.contains(word)) {
//                    V.add(word);
//                }
//            } else {
//                parameters.put(feature, count);
//                Nd += count;
//            }
//        }
//        br.close();
//    }
//
//    /**
//     * 计算条件概率
//     */
//    public static String predict(String sentence, String modelFile) throws Exception {
//        loadModel(modelFile);
//        String predLabel = null;
//        double maxValue = Double.NEGATIVE_INFINITY;// 最大类概率（默认值为负无穷小）
//        String[] words = sentence.split(" ");
//        Set<String> labelSet = parameters.keySet(); // 获得标签集
//        for (String label : labelSet) {
//            double tempValue = Math.log(parameters.get(label) / Nd);// 先验概率
//            /**
//             * 先验概率P(c)= 类c下单词总数/整个训练样本的单词总数 parameters .get(label):类别c对应的文档在训练数据集中的计数
//             * Nd:整个训练样本的单词总数
//             */
//            for (String word : words) {
//                String lf = label + "_" + word;
//                // 计算最大似然概率
//                if (allFeatures.containsKey(lf)) {
//                    tempValue += Math.log((double) (allFeatures.get(lf) + 1) / (labelFeatures.get(label) + V.size()));
//                    /**
//                     * 多项式原理 类条件概率P(tk|c)=(类c下单词tk在各个文档中出现过的次数之和+1)/(类c下单词总数+|V|)
//                     * allFeatures.get(lf)：类别c与词语 w共同出现的次数 labelFeatures.get(label) +
//                     * V.size()：类别c下属性总数+该训练文本中词语总数 Laplace Smoothing处理未出现在训练集中的数据 +1
//                     */
//                } else {
//                    tempValue += Math.log((double) (1 / (labelFeatures.get(label) + V.size())));
//                }
//            }
//            if (tempValue > maxValue) {
//                maxValue = tempValue;
//                predLabel = label;
//            }
//        }
//        return predLabel;
//    }
//
//    public static void predictAll() {
//        double accuracy = 0.;
//        int amount = 0;
//        try {
//            File file = new File("E:\\study\\hadoop\\output\\2021131127_模型.txt");
//            file.createNewFile();
//            BufferedWriter out = new BufferedWriter(new FileWriter(file));
//            List<String> testData = Files.readAllLines(Paths.get(testDataFilePath));
//            for (String instance : testData) {
////                System.out.println(instance);
//                String gold = instance.substring(0, instance.indexOf("："));
//                String sentence = instance.substring(instance.indexOf("\t") + 1);
//                String prediction = predict(sentence, modelFilePath);
//                System.out.println("Gold='" + gold + "'\tPrediction='" + prediction + "'");
//                if (gold.equals(prediction)) {
//                    accuracy += 1;
//                }
//                amount += 1;
//                out.write(amount + "\t" + prediction+"\n");
//            }
//            out.flush();
//            out.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        System.out.println("Accuracy = " + accuracy / amount);
//    }
//}


//import java.io.BufferedReader;
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileReader;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//
//import org.apache.hadoop.fs.Path;
//
//public class NB {
//
//    public static void main(String[] args) {
//        // TODO Auto-generated method stub
//        predictAll();
//    }
//
//    /**
//     * 预测模型采用多项式模型
//     */
//   // private static final String modelFilePath = "D:\\JavaProject\\HadoopLearn\\output.txt";
//   // private static final String testDataFilePath = "D:\\JavaProject\\HadoopLearn\\test.txt";
//
//    private static String modelFilePath = "E:\\study\\hadoop\\output\\2021131127_模型.txt";
//    private static String testDataFilePath = "E:\\study\\hadoop\\doc\\test.txt";
//    private static final String outPath="2021131127_预测结果.txt";
//    public static HashMap<String, Integer> parameters = null; // 情感标签集
//    public static double Nd = 0.;// 文件中的总记录数
//    public static HashMap<String, Integer> allFeatures = null;// 整个训练样本的键值对
//    public static HashMap<String, Double> labelFeatures = null;// 某一类别下所有词出现的总数
//    public static HashSet<String> V = null;// 总训练样本的不重复单词
//
//    /**
//     * 对训练数据进行二次处理，得到多项式模型
//     */
//    public static void loadModel(String modelFile) throws Exception {
//        if (parameters != null && allFeatures != null) {
//            return;
//        }
//        parameters = new HashMap<String, Integer>();// 情感标签集
//        allFeatures = new HashMap<String, Integer>();// 全部属性对
//        labelFeatures = new HashMap<String, Double>();// 某一类别下所有词出现的总数
//        V = new HashSet<String>();
//        BufferedReader br = new BufferedReader(new FileReader(modelFile));
//        String line = null;
//        while ((line = br.readLine()) != null) {
//            String feature = line.substring(0, line.indexOf("\t"));
//            Integer count = Integer.parseInt(line.substring(line.indexOf("\t") + 1));
//            if (feature.contains("_")) {
//                allFeatures.put(feature, count);
//                String label = feature.substring(0, feature.indexOf("_"));
//                if (labelFeatures.containsKey(label)) {
//                    labelFeatures.put(label, labelFeatures.get(label) + count);
//                } else {
//                    labelFeatures.put(label, (double) count);
//                }
//                String word = feature.substring(feature.indexOf("_") + 1);
//                if (!V.contains(word)) {
//                    V.add(word);
//                }
//            } else {
//                parameters.put(feature, count);
//                Nd += count;
//            }
//        }
//        br.close();
//    }
//
//    /**
//     * 计算条件概率
//     */
//    public static String predict(String sentence, String modelFile) throws Exception {
//        loadModel(modelFile);
//        String predLabel = null;
//        double maxValue = Double.NEGATIVE_INFINITY;// 最大类概率（默认值为负无穷小）
//        String[] words = sentence.split(" ");
//        Set<String> labelSet = parameters.keySet(); // 获得标签集
//        for (String label : labelSet) {
//            double tempValue = Math.log(parameters.get(label) / Nd);// 先验概率
//            /**
//             * 先验概率P(c)= 类c下单词总数/整个训练样本的单词总数 parameters .get(label):类别c对应的文档在训练数据集中的计数
//             * Nd:整个训练样本的单词总数
//             */
//            for (String word : words) {
//                String lf = label + "_" + word;
//                // 计算最大似然概率
//                if (allFeatures.containsKey(lf)) {
//                    tempValue += Math.log((double) (allFeatures.get(lf) + 1) / (labelFeatures.get(label) + V.size()));
//                    /**
//                     * 多项式原理 类条件概率P(tk|c)=(类c下单词tk在各个文档中出现过的次数之和+1)/(类c下单词总数+|V|)
//                     * allFeatures.get(lf)：类别c与词语 w共同出现的次数 labelFeatures.get(label) +
//                     * V.size()：类别c下属性总数+该训练文本中词语总数 Laplace Smoothing处理未出现在训练集中的数据 +1
//                     */
//                } else {
//                    tempValue += Math.log((double) (1 / (labelFeatures.get(label) + V.size())));
//                }
//            }
//            if (tempValue > maxValue) {
//                maxValue = tempValue;
//                predLabel = label;
//            }
//        }
//        return predLabel;
//    }
//
//    public static void predictAll() {
//        double accuracy = 0.;
//        int amount = 0;
//        try {
//            File file = new File(outPath);
//            file.createNewFile();
//            BufferedWriter out = new BufferedWriter(new FileWriter(file));
//            List<String> testData = Files.readAllLines(Paths.get(testDataFilePath));
//            for (String instance : testData) {
//                String gold = instance.substring(0, instance.indexOf("："));
//                String sentence = instance.substring(instance.indexOf("：") + 1);
//                String prediction = predict(sentence, modelFilePath);
//                System.out.println("Gold='" + gold + "'\tPrediction='" + prediction + "'");
//                if (gold.equals(prediction)) {
//                    accuracy += 1;
//                }
//                amount += 1;
//                out.write(amount + "\t" + prediction+"\n");
//            }
//            out.flush();
//            out.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        System.out.println("Accuracy = " + accuracy / amount);
//    }
//}
