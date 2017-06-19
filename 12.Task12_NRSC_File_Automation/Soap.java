public class SOAPClientSAAJ {

public static void main(String args[]) throws Exception {
    // Create SOAP Connection
    SOAPConnectionFactory soapConnectionFactory = SOAPConnectionFactory.newInstance();
    SOAPConnection soapConnection = soapConnectionFactory.createConnection();

    // Send SOAP Message to SOAP Server
    String url = "http://ws.cdyne.com/emailverify/Emailvernotestemail.asmx";
    SOAPMessage soapResponse = soapConnection.call(createSOAPRequest(), url);



    SOAPPart soapPart=soapResponse.getSOAPPart();
    // SOAP Envelope
    SOAPEnvelope envelope=soapPart.getEnvelope();
    SOAPBody soapBody = envelope.getBody();

    @SuppressWarnings("unchecked")
    Iterator<Node> itr=soapBody.getChildElements();
    while (itr.hasNext()) {

        Node node=(Node)itr.next();
        if (node.getNodeType()==Node.ELEMENT_NODE) {
            System.out.println("reading Node.ELEMENT_NODE");
            Element ele=(Element)node;
            System.out.println("Body childs : "+ele.getLocalName());

            switch (ele.getNodeName()) {

          case "VerifyEmailResponse":
             NodeList statusNodeList = ele.getChildNodes();

             for(int i=0;i<statusNodeList.getLength();i++){
               Element emailResult = (Element) statusNodeList.item(i);
               System.out.println("VerifyEmailResponse childs : "+emailResult.getLocalName());
                switch (emailResult.getNodeName()) {

                case "VerifyEmailResult":
                    NodeList emailResultList = emailResult.getChildNodes();

                    for(int j=0;j<emailResultList.getLength();j++){
                      Element emailResponse = (Element) emailResultList.item(j);
                      System.out.println("VerifyEmailResult childs : "+emailResponse.getLocalName());
                       switch (emailResponse.getNodeName()) {
                       case "ResponseText":
                           System.out.println(emailResponse.getTextContent());
                           break;
                        case "ResponseCode":
                          System.out.println(emailResponse.getTextContent());
                           break;
                        case "LastMailServer":
                          System.out.println(emailResponse.getTextContent());
                          break;
                        case "GoodEmail":
                          System.out.println(emailResponse.getTextContent());
                       default:
                          break;
                       }
                    }
                    break;
                default:
                   break;
                }
             }


             break;


          default:
             break;
          }

        } else if (node.getNodeType()==Node.TEXT_NODE) {
            System.out.println("reading Node.TEXT_NODE");
            //do nothing here most likely, as the response nearly never has mixed content type
            //this is just for your reference
        }
    }
    // print SOAP Response
    System.out.println("Response SOAP Message:");
    soapResponse.writeTo(System.out);

    soapConnection.close();
}

private static SOAPMessage createSOAPRequest() throws Exception {

    MessageFactory messageFactory = MessageFactory.newInstance();
    SOAPMessage soapMessage = messageFactory.createMessage();
    SOAPPart soapPart = soapMessage.getSOAPPart();

    String serverURI = "http://ws.cdyne.com/";

    // SOAP Envelope
    SOAPEnvelope envelope = soapPart.getEnvelope();
    envelope.addNamespaceDeclaration("example", serverURI);

    /*
    Constructed SOAP Request Message:
    <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:example="http://ws.cdyne.com/">
        <SOAP-ENV:Header/>
        <SOAP-ENV:Body>
            <example:VerifyEmail>
                <example:email>mutantninja@gmail.com</example:email>
                <example:LicenseKey>123</example:LicenseKey>
            </example:VerifyEmail>
        </SOAP-ENV:Body>
    </SOAP-ENV:Envelope>
     */

    // SOAP Body
    SOAPBody soapBody = envelope.getBody();
    SOAPElement soapBodyElem = soapBody.addChildElement("VerifyEmail", "example");
    SOAPElement soapBodyElem1 = soapBodyElem.addChildElement("email", "example");
    soapBodyElem1.addTextNode("mutantninja@gmail.com");
    SOAPElement soapBodyElem2 = soapBodyElem.addChildElement("LicenseKey", "example");
    soapBodyElem2.addTextNode("123");

    MimeHeaders headers = soapMessage.getMimeHeaders();
    headers.addHeader("SOAPAction", serverURI  + "VerifyEmail");

    soapMessage.saveChanges();

    /* Print the request message */
    System.out.println("Request SOAP Message:");
    soapMessage.writeTo(System.out);
    System.out.println("");
    System.out.println("------");

    return soapMessage;
}
}