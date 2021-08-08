#include "store/bftsmartstore/bftsmartagent.h"

namespace bftsmartstore{

JavaVM *BftSmartAgent::jvm;
JNIEnv *BftSmartAgent::env;
// client initialization
BftSmartAgent::BftSmartAgent(bool is_client, TransportReceiver* receiver, int id, int group_idx): is_client(is_client){
    // create Java VM
    create_java_vm();
    if (is_client){
        // generating the config home
        std::ostringstream sstream;

        //sstream << "/users/zw494/java-config/java-config-group-" << group_idx << "/";

        sstream << remote_home << "/java-config/java-config-group-" << group_idx << "/";

        std::string cpp_config_home = sstream.str();
        // create bft interface client
        create_interface_client(receiver, id, cpp_config_home);
    }
    else {
        // create bft interface client
        create_interface_server(receiver, id);
        Debug("finished creating an interface server...");
        // register natives
        register_natives();
    }
}

BftSmartAgent::~BftSmartAgent(){
    Debug("bft smart destructor called!");
    if (is_client){
        jclass cls = BftSmartAgent::env->GetObjectClass(this->bft_client);
        jmethodID mid = BftSmartAgent::env->GetMethodID(cls, "destructBftClient", "()V");
        Debug("calling void destruct method!");
        BftSmartAgent::env->CallVoidMethod(this->bft_client, mid);
        BftSmartAgent::env->DeleteLocalRef(this->bft_client);
        Debug("finished!");
    }
    else {
        BftSmartAgent::env->DeleteLocalRef(this->bft_server);
    }
}

bool BftSmartAgent::create_java_vm(){
    using namespace std;
    if (BftSmartAgent::jvm != nullptr) return true;
    JavaVM *this_jvm;
    JNIEnv *this_env;
    JavaVMInitArgs vm_args;
    JavaVMOption* options = new JavaVMOption[3];

    // std::ostringstream sstream;
    // sstream << "-Djava.class.path=" << remote_home << "/jars/BFT-SMaRt.jar:";
    // sstream << remote_home << "/jars/slf4j-api-1.7.25.jar:";
    // sstream << remote_home << "/jars/bcpkix-jdk15on-160.jar:";
    // sstream << remote_home << "/jars/commons-codec-1.11.jar:";
    // sstream << remote_home << "/jars/logback-classic-1.2.3.jar:";
    // sstream << remote_home << "/jars/netty-all-4.1.34.Final.jar:";
    // sstream << remote_home << "/jars/bcprov-jdk15on-160.jar:";
    // sstream << remote_home << "/jars/core-0.1.4.jar:";
    // sstream << remote_home << "/jars/logback-core-1.2.3.jar:";
    // sstream << remote_home << "/java-config";
    // options[0].optionString = (char *)sstream.str().c_str();
    // sstream.str("");
    // sstream << "-Dlogback.configurationFile=\"" << remote_home << "/java-config/logback.xml\"";
    // options[1].optionString = (char *)sstream.str().c_str();
    // sstream.str("");
    // sstream << "-Djava.security.properties=\"" << remote_home << "/java-config/java.security\"";
    // options[2].optionString = (char *)sstream.str().c_str();
    //
    // std::cerr << "option 1: " << options[0].optionString << std::endl;
    // std::cerr << "option 2: " << options[1].optionString << std::endl;
    // std::cerr << "option 3: " << options[2].optionString << std::endl;

    options[0].optionString = "-Djava.class.path=/users/zw494/jars/BFT-SMaRt.jar:/users/zw494/jars/slf4j-api-1.7.25.jar:/users/zw494/jars/bcpkix-jdk15on-160.jar:/users/zw494/jars/commons-codec-1.11.jar:/users/zw494/jars/logback-classic-1.2.3.jar:/users/zw494/jars/netty-all-4.1.34.Final.jar:/users/zw494/jars/bcprov-jdk15on-160.jar:/users/zw494/jars/core-0.1.4.jar:/users/zw494/jars/logback-core-1.2.3.jar:/users/zw494/java-config";
    options[1].optionString = "-Dlogback.configurationFile=\"/users/zw494/java-config/logback.xml\"";
    options[2].optionString = "-Djava.security.properties=\"/users/zw494/java-config/java.security\"";

    // options[3].optionString = "-Dio.netty.tryReflectionSetAccessible=true";

    vm_args.version = JNI_VERSION_1_6;             // minimum Java version
    vm_args.nOptions = 3;                          // number of options
    vm_args.options = options;
    vm_args.ignoreUnrecognized = false;     // invalid options make the JVM init fail

    //=============== load and initialize Java VM and JNI interface =============
    jint rc = JNI_CreateJavaVM(&this_jvm, (void**)&this_env, &vm_args);  // YES !!

    if (rc != JNI_OK) {
        // TO DO: error processing...
        // cin.get();
        // exit(EXIT_FAILURE);
        return false;
    }
    //=============== Display JVM version =======================================
    Debug("JVM load succeeded: Version ");
    jint ver = this_env->GetVersion();
    Debug("%d.%d", ((ver>>16)&0x0f), (ver&0x0f));
    BftSmartAgent::jvm = this_jvm;
    BftSmartAgent::env = this_env;
    delete options;    // we then no longer need the initialisation options.

    return true;
}

bool BftSmartAgent::create_interface_client(TransportReceiver* receiver, int client_id, std::string cpp_config_home){
    jclass cls = BftSmartAgent::env->FindClass("bftsmart/demo/bftinterface/BftInterfaceClient");  // try to find the class
    if(cls == nullptr) {
        std::cerr << "ERROR: class not found !" << std::endl;
        return false;
    }
    else {
        // if class found, continue
        Debug("Class BftInterfaceClient found. Client ID: %d", client_id);
        jmethodID mid = BftSmartAgent::env->GetMethodID(cls, "<init>", "(IJLjava/lang/String;Ljava/lang/String;)V");  // find method
        if(mid == nullptr){
            std::cerr << "ERROR: constructor not found !" << std::endl;
            return false;
        }
        else {
            jstring config_base = BftSmartAgent::env->NewStringUTF(remote_home.c_str());
            jstring config_home = BftSmartAgent::env->NewStringUTF(cpp_config_home.c_str());
            Debug("successfully created a string!");
            // call method
            this->bft_client = BftSmartAgent::env->NewObject(cls, mid,
                                                            static_cast<jint>(client_id),
                                                            reinterpret_cast<jlong>(receiver),
                                                            config_home,
                                                            config_base);
            if (this->bft_client == nullptr) return false;
        }
    }
    Debug("successfully created BFT interface client!");
    return true;
}

bool BftSmartAgent::create_interface_server(TransportReceiver* receiver, int server_id){
    jclass cls = BftSmartAgent::env->FindClass("bftsmart/demo/bftinterface/BftInterfaceServer");  // try to find the class
    if(cls == nullptr) {
        std::cerr << "ERROR: class not found !" << std::endl;
        return false;
    }
    else {                                  // if class found, continue
       Debug("Class BftInterfaceServer found. Server ID: %d", server_id);

       jmethodID mid = BftSmartAgent::env->GetMethodID(cls, "<init>", "(IJLjava/lang/String;)V");  // find method
        if(mid == nullptr){
            std::cerr << "ERROR: constructor not found !" << std::endl;
            return false;
        }
        else {
            Debug("method ID found!");
            jstring config_base = BftSmartAgent::env->NewStringUTF(remote_home.c_str());
            Debug("successfully created a string!");
            this->bft_server = BftSmartAgent::env->NewObject(cls, mid,
                                                            static_cast<jint>(server_id),
                                                            reinterpret_cast<jlong>(receiver),
                                                            config_base); // call method
            Debug("new bftsmart server object created! Yeeah!");
        }
    }
    return true;
}

void agent_request_received(JNIEnv* env, jobject arr){


    jclass server_cls = env->GetObjectClass(arr);
    jfieldID fid = env->GetFieldID(server_cls, "buffer", "Ljava/nio/ByteBuffer;");
    jobject buffer = env->GetObjectField(arr, fid);
    fid = env->GetFieldID(server_cls, "callbackHandle", "J");
    jlong handle = env->GetLongField(arr, fid);
    TransportReceiver* replica = reinterpret_cast<TransportReceiver*>(handle);
    ReplTransportAddress* repl_addr = new ReplTransportAddress("client", "");

    jlong capacity = env->GetDirectBufferCapacity(buffer);

    // jmethodID mid = env->GetMethodID(buffer_cls, "position", "()I");
    // Debug("getting mid!");

    // jint capacity = env->CallIntMethod(arr, mid);

    Debug("capacity: %d", capacity);

    char* req = static_cast<char*>(env->GetDirectBufferAddress(buffer));

    uint32_t *magic = reinterpret_cast<uint32_t*>(req);
    UW_ASSERT(*magic == MAGIC);

    size_t *sz = (size_t*) (req + sizeof(*magic));

    size_t totalSize = *sz;
    UW_ASSERT(totalSize < 1073741826);
    UW_ASSERT(totalSize == capacity);

    // Parse message
    char *ptr = req + sizeof(*sz) + sizeof(*magic);

    size_t typeLen = *((size_t *)ptr);
    ptr += sizeof(size_t);
    UW_ASSERT((size_t)(ptr-req) < totalSize);

    UW_ASSERT((size_t)(ptr+typeLen-req) < totalSize);
    string msgType(ptr, typeLen);
    ptr += typeLen;

    size_t msgLen = *((size_t *)ptr);
    ptr += sizeof(size_t);
    UW_ASSERT((size_t)(ptr-req) < totalSize);

    UW_ASSERT((size_t)(ptr+msgLen-req) <= totalSize);
    string msg(ptr, msgLen);
    ptr += msgLen;
    Debug("start sending the message to the receiver!");
    // replica->ReceiveFromBFTSmart(msgType, msg);
    replica->ReceiveMessage(*repl_addr, msgType, msg, nullptr);
}

bool BftSmartAgent::register_natives(){
    jclass cls = BftSmartAgent::env->FindClass("bftsmart/demo/bftinterface/BftInterfaceServer");
    Debug("register natives started!");
    JNINativeMethod methods[] { { "bftRequestReceived", "(Lbftsmart/demo/bftinterface/BftInterfaceServer;)V", reinterpret_cast<void *>(agent_request_received) }};  // mapping table
    Debug("native registering!");

    if(BftSmartAgent::env->RegisterNatives(cls, methods, sizeof(methods)/sizeof(JNINativeMethod)) < 0) {                        // register it
        if(BftSmartAgent::env->ExceptionOccurred())                                        // verify if it's ok
        {
            Debug(" OOOOOPS: exception when registering natives");
            return false;
        }
        else
        {
            Debug(" ERROR: problem when registering natives");
            return false;
        }
    }
    Debug("succeeded in registering natives! ");
    return true;
}

void BftSmartAgent::send_to_group(ShardClient* recv, int group_idx, void * buffer, size_t size){
    // this->shard_client = recv;
    Debug("calling send to group!");
    jbyteArray java_byte_array = BftSmartAgent::env->NewByteArray(size);
    BftSmartAgent::env->SetByteArrayRegion(java_byte_array, 0, size, reinterpret_cast<jbyte*>(buffer));

    jclass cls = BftSmartAgent::env->GetObjectClass(this->bft_client);
    jmethodID mid = BftSmartAgent::env->GetMethodID(cls, "startInterface", "([B)V");
    if (mid == nullptr){
        Debug("failed to create mid!");
        return;
    }
    else Debug("successfully found mid!");

    BftSmartAgent::env->CallVoidMethod(this->bft_client, mid, java_byte_array);

}

void BftSmartAgent::destroy_java_vm(){
    jclass cls = BftSmartAgent::env->FindClass("java/lang/System");
    jmethodID mid = BftSmartAgent::env->GetStaticMethodID(cls, "exit", "(I)V");
    BftSmartAgent::env->CallStaticVoidMethod(cls, mid, static_cast<jint>(0));
    BftSmartAgent::jvm->DestroyJavaVM();
    Debug("finished destroying java vm!");
}

}
