package fct.thesis.databaseOCCRDIAS;

/**
 * Created by gomes on 29/04/15.
 */
public class LockHelper {

    final static long lock_mask   = 0x8000000000000000L;
    final static long unlock_mask = 0x00FFFFFFFFFFFFFFL;
    final static long tid_lock    = 0x7F00000000000000L;
    final static long ver_mask    = 0x00FFFFFFFFFFFFFFL;

    public static boolean isLocked(long obj){
        return ((obj& lock_mask)>>63)*-1L==1;
    }

    public static long lock(long obj, long thread_id){
        return (obj|lock_mask) | thread_id<<56;
    }

    public static long unlock(long obj){
        return obj&unlock_mask;
    }

    public static long getThreadId(long obj){
        return (obj&tid_lock)>>56;
    }

    public static long setVersion(long version, long obj){
        return (obj|ver_mask)&version;
    }

    public static long getVersion(long obj){
        return obj&ver_mask;
    }

}
