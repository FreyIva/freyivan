package ru.stroycontrol.master

import android.content.Context
import android.content.SharedPreferences

object Preferences {
    private const val PREFS = "stroycontrol_prefs"
    private const val KEY_SERVER_URL = "server_url"

    fun getServerUrl(context: Context): String {
        return getPrefs(context).getString(KEY_SERVER_URL, "") ?: ""
    }

    fun setServerUrl(context: Context, url: String) {
        getPrefs(context).edit().putString(KEY_SERVER_URL, url).apply()
    }

    private fun getPrefs(context: Context): SharedPreferences {
        return context.getSharedPreferences(PREFS, Context.MODE_PRIVATE)
    }
}
