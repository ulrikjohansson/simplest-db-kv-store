import io.ulrik.db.Db
import io.ulrik.db.FileStore
import io.ulrik.db.FileStoreWithHashMap
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.Test
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
    }

    @Test
    fun testListStore() {
        val db = Db()

        addDataToDb(db)

        assertEquals("2", db.get("y"))
        assertEquals(z, db.get("z"))
        assertEquals("3", db.get("x"))
    }

    @Test
    fun testFileStore() {
        val store = FileStore(kotlin.io.path.createTempFile("db_"))
        val db = Db(store)

        addDataToDb(db)

        assertEquals("2", db.get("y"))
        assertEquals(z, db.get("z"))
        assertEquals("3", db.get("x"))
    }

    @Test
    fun testFileStoreWithHashmap() {
        val store = FileStoreWithHashMap(kotlin.io.path.createTempFile("db_"))
        val db = Db(store)

        addDataToDb(db)

        assertEquals("2", db.get("y"))
        assertEquals(z, db.get("z"))
        assertEquals("3", db.get("x"))
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
    }

    @OptIn(ExperimentalTime::class)
    @Test
    fun testCreateSnapshotFromBigFile() {
        val filestore_path = kotlin.io.path.Path("src/test/resources/filestore.db")

        val fileStoreWithHashMapResult = measureTimedValue { FileStoreWithHashMap(filestore_path, kotlin.io.path.createTempFile("filestore.db")) }
        val fileStoreWithHashMapDb = Db(fileStoreWithHashMapResult.value)
        println("FileStoreWithHashmap load duration: ${fileStoreWithHashMapResult.duration}")

        assertEquals("c83fcf4b-13d5-4abe-b946-6d7c9bfc51a0", fileStoreWithHashMapDb.get("x"))
    }

    @OptIn(ExperimentalTime::class)
    @Test
    fun testSearchThroughBigFile() {
        val filestore_path = kotlin.io.path.Path("src/test/resources/filestoreWithSnapshot.db")

        val fileStore = FileStore(filestore_path)
        val filestoreDb = Db(fileStore)
        val fileStoreWithHashMap = FileStoreWithHashMap(filestore_path)
        val fileStoreWithHashMapDb = Db(fileStoreWithHashMap)

        val filestoreResult = measureTimedValue { filestoreDb.get("x") }
        val fileStoreWithHashMapResult = measureTimedValue { fileStoreWithHashMapDb.get("x") }
        println("FileStore Get duration: ${filestoreResult.duration}")
        println("FileStoreWithHashmap Get duration: ${fileStoreWithHashMapResult.duration}")
    }
}
