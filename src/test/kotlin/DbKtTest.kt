import io.ulrik.db.Db
import io.ulrik.db.FileStore
import io.ulrik.db.FileStoreWithHashMap
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

internal class DbKtTest {

    val z = Json.encodeToString(listOf(mapOf(1 to "This is a string, with some punctuation.")))

    fun addDataToDb(db: Db) {

        db.put("x", "1")
        db.put("y", "2")
        db.put("z", z)
        db.put("x", "3")
        db.put("a", "🇸🇪")
    }

    @Test
    fun testListStore() {
        val db = Db()

        addDataToDb(db)

        assertEquals("2", db.get("y"))
        assertEquals(z, db.get("z"))
        assertEquals("3", db.get("x"))
        assertEquals("🇸🇪", db.get("a"))
    }

    @Test
    fun testFileStore() {
        val store = FileStore(kotlin.io.path.createTempFile("db_"))
        val db = Db(store)

        addDataToDb(db)

        assertEquals("2", db.get("y"))
        assertEquals(z, db.get("z"))
        assertEquals("3", db.get("x"))
        assertEquals("🇸🇪", db.get("a"))
    }

    @Test
    fun testFileStoreWithHashmap() {
        val store = FileStoreWithHashMap(kotlin.io.path.createTempFile("db_"))
        val db = Db(store)

        addDataToDb(db)

        assertEquals("2", db.get("y"))
        assertEquals(z, db.get("z"))
        assertEquals("3", db.get("x"))
        assertEquals("🇸🇪", db.get("a"))
    }

    @Test
    fun testFileStoreWithHashmapInit() {
        val file = kotlin.io.path.createTempFile("db_")
        val store = FileStoreWithHashMap(file)
        val db = Db(store)

        addDataToDb(db)

        val store2 = FileStoreWithHashMap(file)
        val db2 = Db(store2)

        assertEquals("2", db2.get("y"))
        assertEquals(z, db2.get("z"))
        assertEquals("3", db2.get("x"))
        assertEquals("🇸🇪", db.get("a"))
    }

    @OptIn(ExperimentalTime::class)
    @Test
    fun testCreateSnapshotFromBigFile() {
        val filestore_path = kotlin.io.path.Path("src/test/resources/filestore.db")

        val fileStoreWithHashMapResult = measureTimedValue { FileStoreWithHashMap(filestore_path, kotlin.io.path.createTempFile("filestore_temp.db")) }
        val fileStoreWithHashMapDb = Db(fileStoreWithHashMapResult.value)
        println("FileStoreWithHashmap load duration: ${fileStoreWithHashMapResult.duration}")

        assertEquals("3a029bac-af2e-4f26-a8d6-8ca9ef912c52", fileStoreWithHashMapDb.get("x"))
    }

    @OptIn(ExperimentalTime::class)
    @Test
    fun testSearchThroughBigFile() {
        val filestore_path = kotlin.io.path.Path("src/test/resources/filestore.db")

        val fileStore = FileStore(filestore_path)
        val filestoreDb = Db(fileStore)
        val fileStoreWithHashMap = FileStoreWithHashMap(filestore_path)
        val fileStoreWithHashMapDb = Db(fileStoreWithHashMap)

        val filestoreResult = measureTimedValue { filestoreDb.get("x") }
        val fileStoreWithHashMapResult = measureTimedValue { fileStoreWithHashMapDb.get("x") }
        println("FileStore Get duration: ${filestoreResult.duration}")
        println("FileStoreWithHashmap Get duration: ${fileStoreWithHashMapResult.duration}")
    }

    /**
     * Run this to recreate the test filestore if needed (if serialization changes for example)
     */
    @Disabled
    @Test
    fun createBigFileAndSnapshot() {
        val filestore_path = kotlin.io.path.Path("src/test/resources/filestore.db")

        val store = FileStoreWithHashMap(filestore_path)
        val db = Db(store)

        (0..10000).forEach {
            db.put("x", UUID.randomUUID().toString())
        }
    }
}
