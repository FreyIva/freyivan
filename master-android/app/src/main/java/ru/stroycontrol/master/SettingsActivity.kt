package ru.stroycontrol.master

import android.os.Bundle
import android.widget.Toast
import androidx.appcompat.app.AppCompatActivity
import com.google.android.material.button.MaterialButton
import com.google.android.material.textfield.TextInputEditText

class SettingsActivity : AppCompatActivity() {

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_settings)
        findViewById<com.google.android.material.appbar.MaterialToolbar>(R.id.toolbar).setNavigationOnClickListener { finish() }

        val input = findViewById<TextInputEditText>(R.id.server_url)
        input.setText(Preferences.getServerUrl(this))
        input.setSelection(input.text?.length ?: 0)

        findViewById<MaterialButton>(R.id.save_btn).setOnClickListener {
            val url = input.text?.toString()?.trim() ?: ""
            if (url.isBlank()) {
                Toast.makeText(this, R.string.error_no_url, Toast.LENGTH_SHORT).show()
                return@setOnClickListener
            }
            Preferences.setServerUrl(this, url)
            Toast.makeText(this, "Сохранено", Toast.LENGTH_SHORT).show()
            finish()
        }
    }

    override fun onSupportNavigateUp(): Boolean {
        finish()
        return true
    }
}
