#ifdef SWIGJAVA

%typemap(jni) ByteArray "jbyteArray"
%typemap(jtype) ByteArray "byte[]"
%typemap(jstype) ByteArray "byte[]"
%typemap(in) ByteArray {
    $1.Set((std::uint8_t*)JCALL2(GetByteArrayElements, jenv, $input, 0), 
           JCALL1(GetArrayLength, jenv, $input), false);
}
%typemap(out) ByteArray {
    $result = JCALL1(NewByteArray, jenv, $1.Length());
    JCALL4(SetByteArrayRegion, jenv, $result, 0, $1.Length(), (jbyte *)$1.Data());
}
%typemap(javain) ByteArray "$javainput"
%typemap(javaout) ByteArray { return $jnicall; }

%typemap(jni) std::shared_ptr<QueryResult> "jobjectArray"
%typemap(jtype) std::shared_ptr<QueryResult> "BasicResult[]"
%typemap(jstype) std::shared_ptr<QueryResult> "BasicResult[]"
%typemap(out) std::shared_ptr<QueryResult> {
    jclass retClass = jenv->FindClass("BasicResult");
    int len = $1->GetResultNum();
    $result = jenv->NewObjectArray(len, retClass, NULL);
    for (int i = 0; i < len; i++) {
        auto& meta = $1->GetMetadata(i);
        jbyteArray bptr = jenv->NewByteArray(meta.Length());
        jenv->SetByteArrayRegion(bptr, 0, meta.Length(), (jbyte *)meta.Data());
        jenv->SetObjectArrayElement(jresult, i, jenv->NewObject(retClass, jenv->GetMethodID(retClass, "<init>", "(IF[B)V"), (jint)($1->GetResult(i)->VID), (jfloat)($1->GetResult(i)->Dist), bptr));
    }
}
%typemap(javaout) std::shared_ptr<QueryResult> { return $jnicall; }

%typemap(jni) std::shared_ptr<RemoteSearchResult> "jobjectArray"
%typemap(jtype) std::shared_ptr<RemoteSearchResult> "BasicResult[]"
%typemap(jstype) std::shared_ptr<RemoteSearchResult> "BasicResult[]"
%typemap(out) std::shared_ptr<RemoteSearchResult> {
    int combinelen = 0;
    int nodelen = (int)(($1->m_allIndexResults).size());
    for (int i = 0; i < nodelen; i++) {
        combinelen += $1->m_allIndexResults[i].m_results.GetResultNum();
    }
    jclass retClass = jenv->FindClass("BasicResult");
    $result = jenv->NewObjectArray(combinelen, retClass, NULL);
    int id = 0;
    for (int i = 0; i < nodelen; i++) {
        for (int j = 0; j < $1->m_allIndexResults[i].m_results.GetResultNum(); j++) {
            auto& ptr = $1->m_allIndexResults[i].m_results;
            auto& meta = ptr.GetMetadata(j);
            jbyteArray bptr = jenv->NewByteArray(meta.Length());
            jenv->SetByteArrayRegion(bptr, 0, meta.Length(), (jbyte *)meta.Data());
            jenv->SetObjectArrayElement(jresult, id, jenv->NewObject(retClass, jenv->GetMethodID(retClass, "<init>", "(IF[B)V"), (jint)(ptr.GetResult(j)->VID), (jfloat)(ptr.GetResult(j)->Dist), bptr));
            id++;
        }
    }
}
%typemap(javaout) std::shared_ptr<RemoteSearchResult> {
    return $jnicall;
}

#endif
