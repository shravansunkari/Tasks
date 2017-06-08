md bin
javac.exe -d bin src/org/json/*.java
javac.exe -d bin src/org/base64/*.java
javac.exe -d bin -cp bin src/com/DeathByCaptcha/*.java
javac.exe -d bin -cp "bin;lib/*;" src/pro/scraping/captcha/solve/*.java
cd bin
jar.exe cf ../captcha-solver.jar *