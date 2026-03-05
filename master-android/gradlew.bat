@rem Run: gradle wrapper (once), then gradlew assembleDebug
@if exist "%~dp0gradle\wrapper\gradle-wrapper.jar" (
  java -Dorg.gradle.daemon=true -jar "%~dp0gradle\wrapper\gradle-wrapper.jar" %*
) else (
  echo Run: gradle wrapper
  exit /b 1
)
