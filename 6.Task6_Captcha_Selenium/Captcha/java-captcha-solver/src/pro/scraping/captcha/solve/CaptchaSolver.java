package pro.scraping.captcha.solve;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.imageio.ImageIO;

import org.openqa.selenium.Dimension;
import org.openqa.selenium.OutputType;
import org.openqa.selenium.Point;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

import com.DeathByCaptcha.Captcha;
import com.DeathByCaptcha.Exception;
import com.DeathByCaptcha.SocketClient;

public class CaptchaSolver {

	static {
		logger = Logger.getLogger(CaptchaSolver.class.getName());
	}

	public static void main(String[] args) {
		logger.info("Initializing the Firefox webdriver...");
		FirefoxDriver driver = new FirefoxDriver();
		driver.manage().timeouts().implicitlyWait(1, TimeUnit.SECONDS);

		try {
			logger.fine("Opening the page...");
			driver.navigate().to("http://testing-ground.scraping.pro/captcha");
			byte[] arrScreen = driver.getScreenshotAs(OutputType.BYTES);
			BufferedImage imageScreen = ImageIO.read(new ByteArrayInputStream(arrScreen));
			WebElement cap = driver.findElementById("captcha");
			Dimension capDimension = cap.getSize();
			Point capLocation = cap.getLocation();
			BufferedImage imgCap = imageScreen.getSubimage(capLocation.x, capLocation.y, capDimension.width, capDimension.height);
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			ImageIO.write(imgCap, "png", os);
			SocketClient client = new SocketClient("user", "password");
			logger.fine("Sending request to DeathByCaptcha...");
			Captcha res = client.decode(new ByteArrayInputStream(os.toByteArray()));
			
			if (res != null && res.isSolved() && res.isCorrect()) {
				driver.findElementByXPath("//input[@name='captcha_code']").sendKeys(res.text);
				driver.findElementByXPath("//input[@name='submit']").click();
				WebElement h4 = driver.findElementByXPath("//div[@id='case_captcha']//h4");
				
				if (!h4.getText().contains("SUCCESSFULLY")) {
					logger.severe("The captcha has been solved incorrectly!");
					client.report(res);
				}
				else {
					logger.info("The captcha has been solved correctly!");
				}
			}
			else {
				logger.severe("Captcha recognition error occured");
			}
		}
		catch (Exception | IOException | InterruptedException e) {
			logger.severe(String.format("IO exception: %s", e.getMessage()));
		}
		finally {
			driver.close();
		}
	}

	/**
	 * The logger.
	 */
	private static Logger logger;
}
