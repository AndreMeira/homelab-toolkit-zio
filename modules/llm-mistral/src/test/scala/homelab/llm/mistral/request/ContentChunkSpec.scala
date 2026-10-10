package homelab.llm.mistral.request


import zio.json.*
import zio.json.ast.Json
import zio.test.*
import zio.{ Chunk, Scope }


/** Each kind of chunk, as Mistral's schema spells it, read and written both ways. */
object ContentChunkSpec extends ZIOSpecDefault:
  import Support.*

  def spec: Spec[TestEnvironment & Scope, Any] = suite("ContentChunk")(
    suite("written")(
      test("words, as a text chunk") {
        assertTrue(ContentChunk.text("hi").toJson == """{"type":"text","text":"hi"}""")
      },
      test("an image, always as the object, which can carry how closely to look") {
        val image = decoded(ContentChunk.Kind.ImageUrl(ContentChunk.Image("https://x/a.png", Some(ContentChunk.Detail.High))))
        assertTrue(image.toJson == """{"type":"image_url","image_url":{"url":"https://x/a.png","detail":"high"}}""")
      },
      test("a document, with the name the model is told") {
        val document = decoded(ContentChunk.Kind.DocumentUrl("https://x/a.pdf", Some("report")))
        assertTrue(
          document.toJson == """{"type":"document_url","document_url":"https://x/a.pdf","document_name":"report"}"""
        )
      },
      test("an uploaded file, by its id") {
        assertTrue(decoded(ContentChunk.Kind.File("f-1")).toJson == """{"type":"file","file_id":"f-1"}""")
      },
      test("audio, as the bare base64 string the schema takes") {
        assertTrue(decoded(ContentChunk.Kind.InputAudio("AAAA")).toJson == """{"type":"input_audio","input_audio":"AAAA"}""")
      },
      test("reasoning, its own chunks nested inside it") {
        val thinking = decoded(ContentChunk.Kind.Thinking(Chunk(ContentChunk.text("hmm")), signature = Some("sig")))
        assertTrue(
          thinking.toJson == """{"type":"thinking","thinking":[{"type":"text","text":"hmm"}],"signature":"sig"}"""
        )
      },
      test("a resource, with what it holds as a chunk of its own") {
        val resource = decoded(ContentChunk.Kind.Resource("file:///notes.md", ContentChunk.text("hi")))
        assertTrue(
          resource.toJson == """{"type":"resource","uri":"file:///notes.md","content":{"type":"text","text":"hi"}}"""
        )
      },
      test("a resource given by reference, with what is known about it") {
        val link = decoded(ContentChunk.Kind.ResourceLink("file:///notes.md", Some(Map("size" -> Json.Num(12)))))
        assertTrue(link.toJson == """{"type":"resource_link","uri":"file:///notes.md","metadata":{"size":12}}""")
      },
      test("a raw chunk, exactly as it was read") {
        val raw = ContentChunk.Raw(Json.Obj("type" -> Json.Str("citation"), "source" -> Json.Str("x")))
        assertTrue(raw.toJson == """{"type":"citation","source":"x"}""")
      },
    ),
    suite("read")(
      test("an image given as a bare string, which the schema also allows") {
        assertTrue(
          """{"type":"image_url","image_url":"https://x/a.png"}""".fromJson[ContentChunk] ==
            Right(decoded(ContentChunk.Kind.ImageUrl(ContentChunk.Image("https://x/a.png"))))
        )
      },
      test("references, whose ids are numbers or strings") {
        assertTrue(
          """{"type":"reference","reference_ids":[1,"doc-2"]}""".fromJson[ContentChunk] ==
            Right(decoded(ContentChunk.Kind.Reference(Chunk(Json.Num(1), Json.Str("doc-2")))))
        )
      },
      test("reasoning that cites a tool keeps the citation raw inside it, since that kind is not named") {
        val read =
          """{"type":"thinking","thinking":[{"type":"tool_reference","tool":"web_search","title":"x"},
            |{"type":"text","text":"so"}],"closed":true}""".stripMargin.fromJson[ContentChunk]
        assertTrue(read.exists {
          case ContentChunk.Decoded(ContentChunk.Kind.Thinking(Chunk(ContentChunk.Raw(_), said), None, Some(true))) =>
            said == ContentChunk.text("so")
          case _                                                                                                    => false
        })
      },
      test("a chunk with a value no named kind takes is kept raw rather than refused") {
        val sent = """{"type":"image_url","image_url":{"url":"https://x/a.png","detail":"original"}}"""
        assertTrue(sent.fromJson[ContentChunk].exists {
          case ContentChunk.Raw(json) => json.toString.contains("original")
          case _                      => false
        })
      },
      test("what it wrote, it reads back as the same chunk") {
        val chunks = Chunk(
          ContentChunk.text("hi"),
          decoded(ContentChunk.Kind.DocumentUrl("https://x/a.pdf")),
          decoded(ContentChunk.Kind.Thinking(Chunk(ContentChunk.text("hmm")), closed = Some(false))),
          decoded(ContentChunk.Kind.Resource("file:///notes.md", ContentChunk.text("hi"))),
          decoded(ContentChunk.Kind.ResourceLink("file:///notes.md")),
        )
        assertTrue(chunks.toJson.fromJson[Chunk[ContentChunk]] == Right(chunks))
      },
    ),
  )

  /** The chunk constructor the tests share. */
  private object Support {

    /** A chunk of a named kind. */
    def decoded(kind: ContentChunk.Kind): ContentChunk = ContentChunk.Decoded(kind)
  }
