module com.uk.xarixa.cloud {
    requires javafx.controls;
    requires javafx.fxml;

    opens com.uk.xarixa.cloud to javafx.fxml;
    exports com.uk.xarixa.cloud;
}