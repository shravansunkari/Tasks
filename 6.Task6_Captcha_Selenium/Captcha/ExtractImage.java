import java.awt.Image;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import javax.imageio.ImageIO;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import com.asprise.util.ocr.OCR;

public class ExtractImage {
 
 WebDriver driver;
 
 @BeforeTest
  public void setUpDriver() {
   driver = new FirefoxDriver();
  }
 
 @Test
 public void start() throws IOException{
   
 /*Navigate to http://www.mythoughts.co.in/2013/10/extract-and-verify-text-from-image.html page
  * and get the image source attribute
  *  
  */  
 driver.get("http://www.mythoughts.co.in/2013/10/extract-and-verify-text-from-image.html");
 String imageUrl=driver.findElement(By.xpath("//*[@id='post-body-5614451749129773593']/div[1]/div[1]/div/a/img")).getAttribute("src");
 System.out.println("Image source path : \n"+ imageUrl);

 URL url = new URL(imageUrl);
 Image image = ImageIO.read(url);
 String s = new OCR().recognizeCharacters((RenderedImage) image);
 System.out.println("Text From Image : \n"+ s);
 System.out.println("Length of total text : \n"+ s.length());
 driver.quit();
    
 /* Use below code If you want to read image location from your hard disk   
  *   
   BufferedImage image = ImageIO.read(new File("Image location"));   
   String imageText = new OCR().recognizeCharacters((RenderedImage) image);  
   System.out.println("Text From Image : \n"+ imageText);  
   System.out.println("Length of total text : \n"+ imageText.length());   
      
   */ 
}

}

