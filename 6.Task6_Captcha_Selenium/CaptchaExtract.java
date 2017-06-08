package captch;

import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import javax.imageio.ImageIO;

public class CaptchaExtract {
	public static void main(String[] args) throws IOException {

		String captchaPath = "/home/srikanth/Desktop/out.jpg";
		String captcha = getText(captchaPath);
		
		if(captcha.length()>0)
			System.out.println("Captcha =" + captcha);
		else
			System.out.println("Unable to extract captcha");

		// getCaptchText();//to get image from website using Selenium web driver
	}

	public static String getText(String imagePath) throws IOException {
		File testFile = new File(imagePath);

		boolean divideSucess = divideImage(testFile, 100, 35);
		if (divideSucess) {
			return startMatching();
		} else {
			System.out.println("Cannot divide image!");
			return "";
		}
	}

	public static String startMatching() throws IOException {
		/*
		 * Image captcha image size in the website 100 * 35
		 * There will be 8 characters including white spaces. So each character takes 100/8 = 12
		 */
		// default width=12 height=35
		StringBuilder sb = new StringBuilder("");

		//Read individual character images from folder
		File source = new File("/home/srikanth/Desktop/crop_img");

		File[] imgs = source.listFiles();
		Arrays.sort(imgs);
		for (int i = 0; i < imgs.length; i++) {
			if (imgs[i].isFile()) {
				// compare this image with all other images in cmp_img and gets
				// matched one
				File cmp = new File("/home/srikanth/Desktop/cmp_img");
				File[] imgs1 = cmp.listFiles();
				float diff = Float.MAX_VALUE;
				File res_file = null;
				for (int j = 0; j < imgs1.length; j++) {
					
					//resize both images to compare pixel values
					resize(imgs[i].getAbsolutePath(),imgs[i].getAbsolutePath(), 12, 35);
					resize(imgs1[j].getAbsolutePath(),imgs1[j].getAbsolutePath(), 12, 35);
					
					//store resized images in a temporary folder. It prevents original images from changing each time
					File a = resize(imgs[i].getAbsolutePath(),
							"/home/srikanth/Desktop/tmp_folder/f1.png", 12, 35);
					File b = resize(imgs1[j].getAbsolutePath(),
							"/home/srikanth/Desktop/tmp_folder/f2.png", 12, 35);
					
					float val = getDiff(a, b, 12, 35);
					if (val < diff) {
						diff = val;
						res_file = imgs1[j];
					}
				}

				if (res_file != null) {
					String fName = res_file.getName().split("\\.")[0];
					if (fName.length() > 1) {
						fName = fName.substring(0, 1);
					}
					sb.append(fName);
				}
			}
		}
		return sb.toString();
	}

	public static float getDiff(File f1, File f2, int width, int height)
			throws IOException {
		BufferedImage bi1 = null;
		BufferedImage bi2 = null;
		bi1 = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
		bi2 = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);

		bi1 = ImageIO.read(f1);
		bi2 = ImageIO.read(f2);
		float diff = 0;
		for (int i = 0; i < width; i++) {
			for (int j = 0; j < height; j++) {
				int rgb1 = bi1.getRGB(i, j);
				int rgb2 = bi2.getRGB(i, j);

				int b1 = rgb1 & 0xff;
				int g1 = (rgb1 & 0xff00) >> 8;
				int r1 = (rgb1 & 0xff0000) >> 16;

				int b2 = rgb2 & 0xff;
				int g2 = (rgb2 & 0xff00) >> 8;
				int r2 = (rgb2 & 0xff0000) >> 16;

				diff += Math.abs(b1 - b2);
				diff += Math.abs(g1 - g2);
				diff += Math.abs(r1 - r2);
			}
		}
		return diff;
	}

	public static boolean divideImage(File imagePath, int width, int height)
			throws IOException {

		boolean result = false;

		try {
			int widthOfEachChar = width / 8;
			int x = 0;
			
			//store resulting character images in a folder
			String path = "/home/srikanth/Desktop/crop_img/";

			x += widthOfEachChar;
			x += 1;
			File fileToWrite = new File(path + "2.png");
			BufferedImage bufferedImage2 = cropImage(imagePath, x, 0,
					widthOfEachChar, height);
			ImageIO.write(bufferedImage2, "png", fileToWrite);

			x += widthOfEachChar;
			x += 1;
			fileToWrite = new File(path + "3.png");
			BufferedImage bufferedImage3 = cropImage(imagePath, x, 0,
					widthOfEachChar, height);
			ImageIO.write(bufferedImage3, "png", fileToWrite);

			x += widthOfEachChar;
			x += 1;
			fileToWrite = new File(path + "4.png");
			BufferedImage bufferedImage4 = cropImage(imagePath, x, 0,
					widthOfEachChar, height);
			ImageIO.write(bufferedImage4, "png", fileToWrite);

			x += widthOfEachChar;
			x += 1;
			fileToWrite = new File(path + "5.png");
			BufferedImage bufferedImage5 = cropImage(imagePath, x, 0,
					widthOfEachChar, height);
			ImageIO.write(bufferedImage5, "png", fileToWrite);

			x += widthOfEachChar;
			x += 1;
			fileToWrite = new File(path + "6.png");
			BufferedImage bufferedImage6 = cropImage(imagePath, x, 0,
					widthOfEachChar, height);
			ImageIO.write(bufferedImage6, "png", fileToWrite);

			x += widthOfEachChar;
			x += 1;
			fileToWrite = new File(path + "7.png");
			BufferedImage bufferedImage7 = cropImage(imagePath, x, 0,
					widthOfEachChar, height);
			ImageIO.write(bufferedImage7, "png", fileToWrite);

			result = true;
		} catch (Exception e) {
		}
		return result;
	}

	private static BufferedImage cropImage(File filePath, int x, int y, int w,
			int h) {

		try {
			BufferedImage originalImgage = ImageIO.read(filePath);
			BufferedImage subImgage = originalImgage.getSubimage(x, y, w, h);

			return subImgage;
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public static File resize(String inputImagePath, String outputImagePath,
			int scaledWidth, int scaledHeight) throws IOException {
		// reads input image
		File inputFile = new File(inputImagePath);
		BufferedImage inputImage = ImageIO.read(inputFile);

		// creates output image
		BufferedImage outputImage = new BufferedImage(scaledWidth,
				scaledHeight, inputImage.getType());

		// scales the input image to the output image
		Graphics2D g2d = outputImage.createGraphics();
		g2d.drawImage(inputImage, 0, 0, scaledWidth, scaledHeight, null);
		g2d.dispose();

		// extracts extension of output file
		String formatName = outputImagePath.substring(outputImagePath
				.lastIndexOf(".") + 1);

		// writes to output file
		ImageIO.write(outputImage, formatName, new File(outputImagePath));

		return new File(outputImagePath);
	}

}