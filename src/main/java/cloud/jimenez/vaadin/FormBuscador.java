package cloud.jimenez.vaadin;

import com.vaadin.annotations.AutoGenerated;
import com.vaadin.annotations.DesignRoot;
import com.vaadin.navigator.View;
import com.vaadin.ui.declarative.Design;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Button;
import com.vaadin.ui.TextField;
import com.vaadin.ui.Image;
import com.vaadin.ui.Label;
import com.vaadin.ui.TabSheet;

/**
 * !! DO NOT EDIT THIS FILE !!
 * <p>
 * This class is generated by Vaadin Designer and will be overwritten.
 * <p>
 * Please make a subclass with logic and additional interfaces as needed,
 * e.g class LoginView extends LoginDesign implements View { }
 */
@DesignRoot
@AutoGenerated
@SuppressWarnings("serial")
public class FormBuscador extends VerticalLayout implements View {
    protected Image imgHome;
    protected Image imgConfi;
    protected VerticalLayout vlyConfiguracion;
    protected Button btnCargarIndice;
    protected Button btnGenerarIndice;
    protected Button btnGenerarRank;
    protected TabSheet tabInformacion;
    protected Label lblIndiceInvertidoInfo;
    protected Label lbEnlacesAleatorios;
    protected Label lblPageRank;
    protected VerticalLayout vlyPrincipal;
    protected TextField txtBusqueda;
    protected Button btnBuscar;
    protected Label lblInfoBusquedad;
    protected VerticalLayout vlyResultados;

    public FormBuscador() {
        Design.read(this);
    }
}
