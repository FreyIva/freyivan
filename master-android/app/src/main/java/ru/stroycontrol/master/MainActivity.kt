package ru.stroycontrol.master

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.graphics.Bitmap
import android.net.Uri
import android.os.Build
import android.os.Bundle
import android.webkit.ValueCallback
import android.webkit.WebChromeClient
import android.webkit.WebResourceRequest
import android.webkit.WebSettings
import android.webkit.WebView
import android.webkit.WebViewClient
import android.widget.Toast
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AppCompatActivity
import androidx.core.content.ContextCompat
import androidx.core.view.isVisible
import androidx.core.content.FileProvider
import com.google.android.material.dialog.MaterialAlertDialogBuilder
import java.io.File

class MainActivity : AppCompatActivity() {

    private lateinit var webView: WebView
    private lateinit var progress: android.widget.ProgressBar
    private lateinit var emptyState: android.widget.TextView

    private var fileChooserCallback: ValueCallback<Array<Uri>>? = null
    private var tempPhotoUri: Uri? = null

    private val requestPermissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestMultiplePermissions()
    ) { permissions ->
        val allGranted = permissions.values.all { it }
        if (allGranted) loadUrl() else showPermissionDenied()
    }

    private val filePickerLauncher = registerForActivityResult(
        ActivityResultContracts.StartActivityForResult()
    ) { result ->
        val callback = fileChooserCallback
        fileChooserCallback = null
        if (result.resultCode == RESULT_OK && result.data != null) {
            val uris = mutableListOf<Uri>()
            result.data?.clipData?.let { clip ->
                for (i in 0 until clip.itemCount) {
                    clip.getItemAt(i).uri?.let { uris.add(it) }
                }
            }
            if (uris.isEmpty()) result.data?.data?.let { uris.add(it) }
            callback?.onReceiveValue(uris.toTypedArray())
        } else {
            callback?.onReceiveValue(null)
        }
    }

    private val takePictureLauncher = registerForActivityResult(
        ActivityResultContracts.TakePicture()
    ) { success ->
        val callback = fileChooserCallback
        fileChooserCallback = null
        if (success && tempPhotoUri != null) {
            callback?.onReceiveValue(arrayOf(tempPhotoUri!!))
        } else {
            callback?.onReceiveValue(null)
        }
        tempPhotoUri = null
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        webView = findViewById(R.id.webview)
        progress = findViewById(R.id.progress)
        emptyState = findViewById(R.id.empty_state)

        findViewById<com.google.android.material.appbar.MaterialToolbar>(R.id.toolbar).apply {
            setOnMenuItemClickListener { item ->
                if (item.itemId == R.id.action_settings) {
                    startActivity(Intent(this@MainActivity, SettingsActivity::class.java))
                    true
                } else false
            }
            inflateMenu(R.menu.main)
        }

        setupWebView()
        checkPermissionsAndLoad()
    }

    override fun onResume() {
        super.onResume()
        if (::webView.isInitialized && getServerUrl().isNotBlank()) {
            loadUrl()
        }
    }

    private fun setupWebView() {
        webView.settings.apply {
            javaScriptEnabled = true
            domStorageEnabled = true
            databaseEnabled = true
            cacheMode = WebSettings.CACHE_MODE_DEFAULT
            mixedContentMode = WebSettings.MIXED_CONTENT_COMPATIBILITY_MODE
            allowFileAccess = true
            allowContentAccess = true
            setSupportZoom(true)
            builtInZoomControls = true
            displayZoomControls = false
            loadWithOverviewMode = true
            useWideViewPort = true
            userAgentString = userAgentString + " StroyControlMaster/1.0"
        }

        webView.webViewClient = object : WebViewClient() {
            override fun onPageStarted(view: WebView?, url: String?, favicon: Bitmap?) {
                progress.isVisible = true
            }

            override fun onPageFinished(view: WebView?, url: String?) {
                progress.isVisible = false
            }

            override fun shouldOverrideUrlLoading(view: WebView?, request: WebResourceRequest?): Boolean {
                return false
            }
        }

        webView.webChromeClient = object : WebChromeClient() {
            override fun onShowFileChooser(
                webView: WebView?,
                filePathCallback: ValueCallback<Array<Uri>>?,
                fileChooserParams: FileChooserParams?
            ): Boolean {
                fileChooserCallback?.onReceiveValue(null)
                fileChooserCallback = filePathCallback
                val acceptTypes = fileChooserParams?.acceptTypes ?: arrayOf("*/*")
                val acceptImages = acceptTypes.any { it.contains("image") || it == "*/*" }

                val options = if (acceptImages && hasCamera()) {
                    arrayOf("Камера", "Галерея", "Отмена")
                } else {
                    arrayOf("Галерея", "Отмена")
                }

                MaterialAlertDialogBuilder(this@MainActivity)
                    .setItems(options) { _, which ->
                        when (options[which]) {
                            "Камера" -> openCamera()
                            "Галерея" -> openGallery(acceptImages)
                            "Отмена" -> {
                                fileChooserCallback?.onReceiveValue(null)
                                fileChooserCallback = null
                            }
                        }
                    }
                    .setOnCancelListener {
                        fileChooserCallback?.onReceiveValue(null)
                        fileChooserCallback = null
                    }
                    .show()
                return true
            }
        }
    }

    private fun hasCamera(): Boolean = packageManager.hasSystemFeature(PackageManager.FEATURE_CAMERA_ANY)

    private fun openCamera() {
        val file = File(cacheDir, "photo_${System.currentTimeMillis()}.jpg")
        tempPhotoUri = FileProvider.getUriForFile(this, "$packageName.fileprovider", file)
        takePictureLauncher.launch(tempPhotoUri!!)
    }

    private fun openGallery(imagesOnly: Boolean) {
        val intent = Intent(Intent.ACTION_GET_CONTENT).apply {
            type = if (imagesOnly) "image/*" else "*/*"
            putExtra(Intent.EXTRA_ALLOW_MULTIPLE, true)
        }
        filePickerLauncher.launch(Intent.createChooser(intent, "Выберите фото"))
    }

    private fun checkPermissionsAndLoad() {
        val permissions = mutableListOf<String>()
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.READ_MEDIA_IMAGES) != PackageManager.PERMISSION_GRANTED) {
                permissions.add(Manifest.permission.READ_MEDIA_IMAGES)
            }
        } else if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.M) {
            if (ContextCompat.checkSelfPermission(this, Manifest.permission.READ_EXTERNAL_STORAGE) != PackageManager.PERMISSION_GRANTED) {
                permissions.add(Manifest.permission.READ_EXTERNAL_STORAGE)
            }
        }
        if (hasCamera() && ContextCompat.checkSelfPermission(this, Manifest.permission.CAMERA) != PackageManager.PERMISSION_GRANTED) {
            permissions.add(Manifest.permission.CAMERA)
        }
        if (permissions.isEmpty()) {
            loadUrl()
        } else {
            requestPermissionLauncher.launch(permissions.toTypedArray())
        }
    }

    private fun showPermissionDenied() {
        Toast.makeText(this, "Нужны разрешения для загрузки фото отчётов", Toast.LENGTH_LONG).show()
        loadUrl()
    }

    private fun loadUrl() {
        val url = getServerUrl().trim()
        if (url.isBlank()) {
            webView.isVisible = false
            emptyState.isVisible = true
            emptyState.text = getString(R.string.error_no_url) + "\n\nНажмите ⋮ → Настройки"
            return
        }
        emptyState.isVisible = false
        webView.isVisible = true
        val base = if (url.endsWith("/")) url.dropLast(1) else url
        webView.loadUrl("$base/login")
    }

    private fun getServerUrl(): String = Preferences.getServerUrl(this)
}
