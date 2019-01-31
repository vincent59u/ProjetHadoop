package fr.miage.matthieu;

import org.apache.commons.io.FileUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.*;

public class Utils {

    /**
     * Méthode qui permet de lire les centroids de départ pour la question5
     * @param filename
     * @return
     * @throws IOException
     */
    public static List<Double[]> readCentroids(String filename) throws IOException
    {
        FileInputStream fileInputStream = new FileInputStream(filename);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream));
        return readData(reader);
    }

    /**
     * Méthode qui permet de lire un fichier de donnée et de placer le contenu dans une liste de tableau
     * @param reader
     * @return
     * @throws IOException
     */
    private static List<Double[]> readData(BufferedReader reader) throws IOException
    {
        List<Double[]> centroids = new LinkedList<>();
        String line;

        while ((line = reader.readLine()) != null) {
            //[index, lat, long]
            String[] tokens = line.split(",");
            Double[] centroid = new Double[2];
            centroid[0] = Double.parseDouble(tokens[1]);
            centroid[1] = Double.parseDouble(tokens[2]);
            centroids.add(centroid);
        }

        reader.close();

        return centroids;
    }

    /**
     * Méthode qui permet de remplacer le contenu d'un fichier en local vers un fichier dans le HDFS
     * @param target
     * @param source
     * @throws IOException
     */
    public static void replaceFileContent(String target, String source) throws IOException
    {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);
        Path filePath = new Path(source);

        File centroidFile = new File(target);
        FileWriter fileWriter = new FileWriter(centroidFile, false);

        FSDataInputStream fsDataInputStream = fs.open(filePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fsDataInputStream));
        String line;

        while ((line = reader.readLine()) != null) {
            fileWriter.write(line + "\n");
        }

        fsDataInputStream.close();
        reader.close();
        fileWriter.close();
    }

    /**
     * Méthode qui permet de vérifier le contenu de deux fichier (l'un en local et l'autre dans l'HDFS)
     * @param file1
     * @param file2
     * @return
     * @throws IOException
     */
    public static boolean checkEqualContent(String file1, String file2) throws IOException
    {
        Configuration configuration = new Configuration();
        LocalFileSystem localFileSystem = FileSystem.getLocal(configuration);
        return FileUtils.contentEquals(new File(file1), new File(String.valueOf(localFileSystem.pathToFile(new Path(file2)))));
    }

    /**
     * Méthode qui permet de trier une HashMap par les valeurs
     * @param map
     * @return
     */
    public static Object[] sortByValue(HashMap<?, ?> map)
    {
        Object[] sorted = map.entrySet().toArray();
        Arrays.sort(sorted, new Comparator() {
            public int compare(Object o1, Object o2) {
                return ((Map.Entry<String, Integer>) o2).getValue()
                        .compareTo(((Map.Entry<String, Integer>) o1).getValue());
            }
        });
        return sorted;
    }

    /**
     * Méthode qui permet de déplacer les fichiers temporaires dans le dossier de data
     * @link https://stackoverflow.com/questions/25622738/adding-output-files-to-an-existing-output-directory-in-mapreduce
     * @param from
     * @param to
     * @param conf
     * @param filename
     * @throws IOException
     */
    public static void moveFiles(Path from, Path to, Configuration conf, String filename) throws IOException {
        FileSystem fs = from.getFileSystem(conf);
        for (FileStatus status : fs.listStatus(from)) {
            Path file = status.getPath();
            if (file.getName().equals(filename)) {
                //On delete le fichier existant dans le dossier des data
                fs.delete(new Path(to.toString() + "/" + filename), true);
                Path dst = new Path(to, file.getName());
                fs.rename(file, dst);
            }
        }
    }
}
