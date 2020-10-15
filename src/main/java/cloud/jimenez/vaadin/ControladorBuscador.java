package cloud.jimenez.vaadin;

import cloud.jimenez.vaadin.IndiceInvertido.InvertedIndexMapper;
import cloud.jimenez.vaadin.IndiceInvertido.invertedIndexReducer;
import cloud.jimenez.vaadin.PageRank.Map;
import cloud.jimenez.vaadin.PageRank.Node;
import cloud.jimenez.vaadin.PageRank.Reduce;
import com.vaadin.server.Page;
import com.vaadin.shared.Position;
import com.vaadin.shared.ui.ContentMode;
import com.vaadin.ui.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;


import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.IOException;
import java.io.OutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import static java.lang.Math.random;

public class ControladorBuscador extends FormBuscador{
    List<String> ignoreWords = new ArrayList<String>();

    String input_i_test = "input_folder_test/";
    String input_i_text = "input_folder_text/";
    String output_o_test = "out_indexinverted_test/";
    String output_o_text = "out_indexinverted_text/";
    String ruta_page_rank;
    String input_index_inverted;
    String output_index_inverted;
    boolean test = false;

    public ControladorBuscador() {

        if(test){
            input_index_inverted = input_i_test;
            output_index_inverted = output_o_test;
        }else{
            input_index_inverted = input_i_text;
            output_index_inverted = output_o_text;
        }

        CargarStepWord();

        setEventos();
    }

    private void CargarStepWord() {
        Path pt = new Path("/home/usuario/Proyectos/UCSP/CloudComputing/MotorBusqueda/ignorewords.txt");
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line = br.readLine();
            while (line != null) {
                ignoreWords.add(line.trim().toLowerCase());
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void setEventos() {

        imgConfi.addClickListener(event ->{
            vlyPrincipal.setVisible(false);
            vlyConfiguracion.setVisible(true);
        });

        imgHome.addClickListener(event ->{
            vlyPrincipal.setVisible(true);
            vlyConfiguracion.setVisible(false);
        });

        btnGenerarIndice.addClickListener(event ->{
            Limpiar();
            try {
                IndiceInvertido();
                PageRank();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        });

        btnCargarIndice.addClickListener(event ->{
            lblIndiceInvertidoInfo.setValue(LeerArchivo(output_index_inverted+"/part-r-00000").getValue());
        });


        btnBuscar.addClickListener(event ->{
           Limpiar();
           if(txtBusqueda.getValue().trim().length() == 0){
               Notification.show("Ingrese el texto que desea buscar", Notification.Type.ERROR_MESSAGE);
           }else{
               CargarResultados();
           }
        });

        btnGenerarRank.addClickListener(event ->{
           PageRank();
        });
    }

    private void IndiceInvertido() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Path inputDirectoryPath = new Path(input_index_inverted);
        Path outputDirectoryPath =  new Path(output_index_inverted);

        conf.set("filePath", "/home/usuario/Proyectos/UCSP/CloudComputing/MotorBusqueda/ignorewords.txt");
        conf.set("key_search",txtBusqueda.getValue());
        Job job = Job.getInstance(conf, "Inverted Index");
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(invertedIndexReducer.class);
        job.setNumReduceTasks(1); //1 output file
        //job.setJarByClass(InvertedIndexDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputValueClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, inputDirectoryPath);
        TextOutputFormat.setOutputPath(job, outputDirectoryPath);

        if(job.waitForCompletion(true)){
            Notification notification = new Notification("Eventos");
            notification.setDescription("<span>Se ha generado el indice invertido para la busquedad</span>");
            notification.setHtmlContentAllowed(true);
            notification.setStyleName("tray dark small closable login-help");
            notification.setPosition(Position.BOTTOM_CENTER);
            notification.setDelayMsec(20000);
            notification.show(Page.getCurrent());

            lblIndiceInvertidoInfo.setValue(LeerArchivo(outputDirectoryPath.getName()+"/part-r-00000").getValue());
        }

        //System.exit(job.waitForCompletion(true)? 0: 1);

    }

    public void LeerArchivosDirectorio(){
        Limpiar();
        try {
            Files.walk(Paths.get("/home/usuario/Resultados/")).forEach(ruta-> {
                if (Files.isRegularFile(ruta)) {
                    System.out.println(ruta);
                    VerticalLayout vly_tmp = new VerticalLayout();
                    vly_tmp.setWidth("100%");

                    Label archivo = new Label(ruta.getFileName().toString());
                    archivo.setData(ruta.getFileName().toString());
                    archivo.setCaption("<b><font color = blue>"+ ruta.getFileName().toString()+ "</font></b>");
                    archivo.setCaptionAsHtml(true);
                    vly_tmp.addComponent(archivo);

                    Panel panel_tmp = new Panel();
                    panel_tmp.setWidth("100%");
                    panel_tmp.setContent(vly_tmp);

                    panel_tmp.addClickListener(event ->{
                        Notification.show("me clicleaste "+ archivo.getData(), Notification.Type.WARNING_MESSAGE);
                        Window windows_tmp = new Window();
                        windows_tmp.setModal(true);
                        windows_tmp.setWidth("80%");
                        VerticalLayout w_vly = new VerticalLayout();
                        w_vly.addComponent(LeerArchivo("/home/usuario/Resultados/"+archivo.getData().toString()));
                        windows_tmp.setContent(w_vly);
                        getUI().getCurrent().addWindow(windows_tmp);
                    });
                    vlyResultados.addComponent(panel_tmp);
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Label LeerArchivo(String archivo) {
        String cadena;
        String resultado = "";
        FileReader f = null;
        try {
            f = new FileReader(archivo);
            BufferedReader b = new BufferedReader(f);
            while((cadena = b.readLine())!=null) {
                resultado += cadena;
                resultado += "<br>";
            }
            b.close();
            } catch (FileNotFoundException e) {
                Notification.show("No se ha generado archivo de indice invertido", Notification.TYPE_ERROR_MESSAGE);
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        Label l = new Label(resultado, ContentMode.HTML);
        l.setWidth("100%");
        return l;
    }



    private void Limpiar() {
        vlyResultados.removeAllComponents();
    }

    private void CargarResultados(){
        List<String> textos_resultado = new ArrayList<>();
        List<String> key_search = new ArrayList<>();
        for(String value : txtBusqueda.getValue().split(" ")){
            if(!ignoreWords.contains(value)){
                key_search.add(value);
            }
        }
        if(key_search.size() > 0){
            lblInfoBusquedad.setValue("<b> Se ha realizado la busquedad con las siguientes claves:</b> "+key_search.toString());
            lblInfoBusquedad.setVisible(true);
        }else{
            lblInfoBusquedad.setValue("<b> Sin resultados para: </b> "+txtBusqueda.getValue());
            lblInfoBusquedad.setVisible(true);
        }

        for(String value : key_search.stream().distinct().collect(Collectors.toList())){
            String cadena;
            FileReader f = null;
            try {
                f = new FileReader(output_index_inverted+"/part-r-00000");
                BufferedReader b = new BufferedReader(f);
                while((cadena = b.readLine())!=null) {
                    /**
                     * Código para que muestre palabras que contengan la clave de busquedad
                     * if(cadena.contains(value))
                     *   textos_resultado += cadena +" ";
                     */
                     for(String value_tmp : cadena.split("\t")){
                         if (value_tmp.equalsIgnoreCase(value)){
                             textos_resultado.add(cadena);
                             break;
                         }
                     }
                }
                b.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if(textos_resultado.size() != 0){
            vlyResultados.setVisible(true);
            java.util.Map<String,Double> datos_ordenados = OrdernarPageRank(textos_resultado);

            /*cuando solo es textos_resultados sin ordenamiento
            for(String value : textos_resultado) {
                for(String ruta : value.split("\t")) {
                    String ruta_texto = input_index_inverted + ruta;
                    System.out.println(ruta_texto);
                    VerticalLayout vly_tmp = new VerticalLayout();
                    vly_tmp.setWidth("100%");

                    Label archivo = new Label(ruta_texto);
                    archivo.setData(ruta_texto);
                    archivo.setCaption("<b><font color = blue>" + ruta_texto + "</font></b>");
                    archivo.setCaptionAsHtml(true);
                    vly_tmp.addComponent(archivo);

                    Panel panel_tmp = new Panel();
                    panel_tmp.setWidth("100%");
                    panel_tmp.setContent(vly_tmp);

                    panel_tmp.addClickListener(event -> {
                        //Notification.show("me clicleaste " + archivo.getData(), Notification.Type.WARNING_MESSAGE);
                        Window windows_tmp = new Window();
                        windows_tmp.setModal(true);
                        windows_tmp.setWidth("80%");
                        VerticalLayout w_vly = new VerticalLayout();
                        w_vly.addComponent(LeerArchivo("/home/usuario/Proyectos/UCSP/CloudComputing/MotorBusqueda/"+archivo.getData().toString()));
                        windows_tmp.setContent(w_vly);
                        getUI().getCurrent().addWindow(windows_tmp);
                    });
                    vlyResultados.addComponent(panel_tmp);
                }
            }
            */

            for(String value_book : datos_ordenados.keySet()) {
                    String ruta_texto = input_index_inverted + value_book;
                    System.out.println(ruta_texto);
                    VerticalLayout vly_tmp = new VerticalLayout();
                    vly_tmp.setWidth("100%");

                    Label archivo = new Label(ruta_texto);
                    archivo.setData(ruta_texto);
                    archivo.setCaption("<b><font color = blue>" + ruta_texto + "</font> <font color = black>PageRank: "+datos_ordenados.get(value_book)+"</font></b>");
                    archivo.setCaptionAsHtml(true);
                    vly_tmp.addComponent(archivo);

                    Panel panel_tmp = new Panel();
                    panel_tmp.setWidth("100%");
                    panel_tmp.setContent(vly_tmp);

                    panel_tmp.addClickListener(event -> {
                        //Notification.show("me clicleaste " + archivo.getData(), Notification.Type.WARNING_MESSAGE);
                        Window windows_tmp = new Window();
                        windows_tmp.setModal(true);
                        windows_tmp.setWidth("80%");
                        VerticalLayout w_vly = new VerticalLayout();
                        w_vly.addComponent(LeerArchivo("/home/usuario/Proyectos/UCSP/CloudComputing/MotorBusqueda/"+archivo.getData().toString()));
                        windows_tmp.setContent(w_vly);
                        getUI().getCurrent().addWindow(windows_tmp);
                    });
                    vlyResultados.addComponent(panel_tmp);

            }

        }else
        {
            Notification.show("No se han encontrado archivos para su busquedad", Notification.Type.ERROR_MESSAGE);
            vlyResultados.setVisible(false);
        }

    }

    private java.util.Map<String,Double> OrdernarPageRank(List<String> textos_resultado) {
        java.util.Map<String,Double> textos_pagerank = new TreeMap<>();
        String cadena;
        FileReader f = null;
        for(String value : textos_resultado) {
            for (String book_page : value.split("\t")) {
                try {
                    f = new FileReader(ruta_page_rank);
                    BufferedReader b = new BufferedReader(f);
                    while ((cadena = b.readLine()) != null) {

                        if (cadena.split("\t")[0].equals(book_page)) {
                            textos_pagerank.put(book_page,Double.parseDouble(cadena.split("\t")[1]));
                        }
                    }
                    b.close();
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        /*
        Iterator it = textos_pagerank.keySet().iterator();
        while(it.hasNext()){
            String key = (String) it.next();
            System.out.println("Clave: " + key + " -> Valor: " + textos_pagerank.get(key));
        }
        */
        return textos_pagerank;
    }

    private void PageRank(){
        try {
            CrearRankAleatorio();
            Notification notification = new Notification("Eventos");
            notification.setDescription("<span>Creando PageRank de los archivos</span>");
            notification.setHtmlContentAllowed(true);
            notification.setStyleName("tray dark small closable login-help");
            notification.setPosition(Position.BOTTOM_CENTER);
            notification.setDelayMsec(20000);
            notification.show(Page.getCurrent());

            iterate("/home/usuario/Proyectos/UCSP/CloudComputing/MotorBusqueda/rank_inicial.txt", "/home/usuario/Proyectos/UCSP/CloudComputing/MotorBusqueda/convergencias");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void iterate(String input, String output) throws Exception {

        Configuration conf = new Configuration();
        Path outputPath = new Path(output);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        outputPath.getFileSystem(conf).mkdirs(outputPath);
        Path inputPath = new Path(outputPath, "input.txt");
        int numNodes = createInputFile(new Path(input), inputPath);
        int iter = 1;
        double desiredConvergence = 0.01;

        while (true) {

            Path jobOutputPath = new Path(outputPath, String.valueOf(iter));

            System.out.println("======================================");
            System.out.println("=  Iteration:    " + iter);
            System.out.println("=  Input path:   " + inputPath);
            System.out.println("=  Output path:  " + jobOutputPath);
            System.out.println("======================================");

            if (calcPageRank(inputPath, jobOutputPath, numNodes) < desiredConvergence) {
                ruta_page_rank = jobOutputPath+"/part-r-00000";
                lblPageRank.setValue(LeerArchivo(jobOutputPath+"/part-r-00000").getValue());
                System.out.println("Convergencia es menor que" + desiredConvergence + ", we're done");
                break;
            }
            inputPath = jobOutputPath;
            iter++;
        }
    }
    public static int createInputFile(Path file, Path targetFile) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = file.getFileSystem(conf);

        int numNodes = getNumNodes(file);
        double initialPageRank = 1.0 / (double) numNodes;

        OutputStream os = fs.create(targetFile);
        LineIterator iter = IOUtils.lineIterator(fs.open(file), "UTF8");

        while (iter.hasNext()) {
            String line = iter.nextLine();

            String[] parts = StringUtils.split(line);

            Node node = new Node()
                    .setPageRank(initialPageRank)
                    .setAdjacentNodeNames(
                            Arrays.copyOfRange(parts, 1, parts.length));
            IOUtils.write(parts[0] + '\t' + node.toString() + '\n', os);
        }
        os.close();
        return numNodes;
    }

    public static int getNumNodes(Path file) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = file.getFileSystem(conf);
        return IOUtils.readLines(fs.open(file), "UTF8").size();
    }

    public static double calcPageRank(Path inputPath, Path outputPath, int numNodes)
            throws Exception {
        Configuration conf = new Configuration();

        conf.setInt(Reduce.CONF_NUM_NODES_GRAPH, numNodes);

        Job job = Job.getInstance(conf, "PageRankJob");
        //job.setJarByClass(Main.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        if (!job.waitForCompletion(true)) {
            throw new Exception("Job failed");
        }

        long summedConvergence = job.getCounters().findCounter(
                Reduce.Counter.CONV_DELTAS).getValue();
        double convergence =
                ((double) summedConvergence /
                        Reduce.CONVERGENCE_SCALING_FACTOR) /
                        (double) numNodes;

        System.out.println("======================================");
        System.out.println("=  Num nodes:           " + numNodes);
        System.out.println("=  Summed convergence:  " + summedConvergence);
        System.out.println("=  Convergence:         " + convergence);
        System.out.println("======================================");

        return convergence;
    }

    private void CrearRankAleatorio(){
        lbEnlacesAleatorios.setValue("");
        List<String> archivos = new ArrayList<>();
        try {
            Files.walk(Paths.get("//home/usuario/Proyectos/UCSP/CloudComputing/MotorBusqueda/input_folder_text")).forEach(ruta-> {
                if (Files.isRegularFile(ruta)) {
                    //System.out.println(ruta.getFileName());
                    archivos.add(ruta.getFileName().toString());
                }
            });
            Notification notification = new Notification("Eventos");
            notification.setDescription("<span>Generando enlaces aleatorio entre los "+ archivos.size()+" archivos</span>");
            notification.setHtmlContentAllowed(true);
            notification.setStyleName("tray dark small closable login-help");
            notification.setPosition(Position.BOTTOM_CENTER);
            notification.setDelayMsec(20000);
            notification.show(Page.getCurrent());

           /* Configuration conf = new Configuration();
            Path outputPath = new Path("/home/usuario/Proyectos/UCSP/CloudComputing/MotorBusqueda/rank_inicial.txt");
            outputPath.getFileSystem(conf).delete(outputPath, true);
            outputPath.getFileSystem(conf).mkdirs(outputPath);
            */
            String ruta = "/home/usuario/Proyectos/UCSP/CloudComputing/MotorBusqueda/rank_inicial.txt";
            File file = new File(ruta);
            FileWriter fw = new FileWriter(file);
            BufferedWriter bw = new BufferedWriter(fw);
            for(String archivo : archivos){
                int numero_enlaces = (int) (random() * 20+1);
                String contenido = archivo + "\t";
                for(int i=0; i< numero_enlaces ; ++i){

                    contenido += archivos.get( (int)(random() * archivos.size()));
                    contenido += "\t";
                }
                bw.write(contenido);
                bw.newLine();
                lbEnlacesAleatorios.setValue(lbEnlacesAleatorios.getValue() + contenido +"<br>");
            }
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
