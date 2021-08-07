import io.ulrik.db.Db
import io.ulrik.db.FileStore
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

internal class DbKtTest {

    @Test
    fun testListStore() {
        val db = Db()

        val z = Json.encodeToString(listOf(mapOf(1 to "This is a string, with some punctuation.")))

        db.put("x", "1")
        db.put("y", "2")
        db.put("z", z)
        db.put("x", "3")

        assertEquals("2", db.get("y"))
        assertEquals(z, db.get("z"))
        assertEquals("3", db.get("x"))
    }

    @Test
    fun testFileStore() {
        val store = FileStore(kotlin.io.path.createTempFile("db_"))
        val db = Db(store)

        val z = Json.encodeToString(listOf(mapOf(1 to "This is a string, with some punctuation.")))

        db.put("x", "1")
        db.put("y", "2")
        db.put("z", z)
        db.put("x", "3")

        assertEquals("2", db.get("y"))
        assertEquals(z, db.get("z"))
        assertEquals("3", db.get("x"))
    }
}
