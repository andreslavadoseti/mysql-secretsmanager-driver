/*
 * 
 */
package co.com.seti.aws.utils.mysql.secretsmanager.driver;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.cj.jdbc.NonRegisteringDriver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.text.StringSubstitutor;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

/**
 * @author andreslavado
 */
public class MySQLSecretsManagerDriver extends NonRegisteringDriver implements java.sql.Driver {
    
    private static final Logger LOGGER = Logger.getLogger(MySQLSecretsManagerDriver.class.getName());
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    private static final String VARIABLE_PREFIX = "${";
    private static final String VARIABLE_SUFFIX = "}";    

    public MySQLSecretsManagerDriver() throws SQLException {}

    static {
        try {
            DriverManager.registerDriver(new MySQLSecretsManagerDriver());
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, null, e);
            throw new RuntimeException("Can't register driver!");
        }
    }
    
    private AwsCredentialsProvider getCredentials(){
        //TODO: Implement another credential options
        return DefaultCredentialsProvider.create();
    }
    
    private Map<String, String> getSecrets(String secretId){
        //TODO: Throw error when secret store dont exist or get secrets fails
        SecretsManagerClient client = SecretsManagerClient.builder().credentialsProvider(getCredentials()).build();
        GetSecretValueRequest request = GetSecretValueRequest.builder().secretId(secretId).build();
        GetSecretValueResponse response = client.getSecretValue(request);
        String secretText = response.secretString();
        if(secretText == null || secretText.isEmpty()){
            //Handle secret binary
            byte[] decodedBytes = Base64.getDecoder().decode(response.secretBinary().asByteArray());
            secretText = new String(decodedBytes);
        }
        Map<String, String> secrets = new HashMap<>();
        try {
            secrets = objectMapper.readValue(secretText, new TypeReference<Map<String,String>>(){});
        } catch (JsonProcessingException ex) {
            LOGGER.log(Level.SEVERE, null, ex);
        }        
        client.close();
        return secrets;
    }

    /**
     *
     * @param url, connection string
     * @param info, connection credentials
     * @return mysql connection
     * @throws SQLException 
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        Map<String, String> secrets = getSecrets(info.get("user").toString());
        Map<String, String> replaceValues = new HashMap<>();
        //Secrets data: username, password, engine, host, port, dbname, dbInstanceIdentifier
        replaceValues.put("db_host", secrets.get("host"));
        replaceValues.put("db_port", secrets.get("port"));
        replaceValues.put("db_name", secrets.get("dbname"));
        String newUrl = StringSubstitutor.replace(url, replaceValues, VARIABLE_PREFIX, VARIABLE_SUFFIX);
        Properties newInfo = new Properties();
        newInfo.put("user", secrets.get("username"));
        newInfo.put("password", secrets.get("password"));
        LOGGER.log(Level.INFO, "Connecting to RDS instance: {0}", secrets.get("dbInstanceIdentifier"));
        return super.connect(newUrl, newInfo);
    }
}
