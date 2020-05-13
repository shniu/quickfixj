/*******************************************************************************
 * Copyright (c) quickfixengine.org  All rights reserved.
 *
 * This file is part of the QuickFIX FIX Engine
 *
 * This file may be distributed under the terms of the quickfixengine.org
 * license as defined by quickfixengine.org and appearing in the file
 * LICENSE included in the packaging of this file.
 *
 * This file is provided AS IS with NO WARRANTY OF ANY KIND, INCLUDING
 * THE WARRANTY OF DESIGN, MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE.
 *
 * See http://www.quickfixengine.org/LICENSE for licensing information.
 *
 * Contact ask@quickfixengine.org if any conditions of this licensing
 * are not clear to you.
 ******************************************************************************/

package quickfix;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper class for delegating message types for various FIX versions to
 * type-safe onMessage methods.
 *
 * 将不同类型和不同版本的消息委托给具体的方法去处理
 */
public class MessageCracker {
    // 暂存所有注册的方法，消息类型 -> 处理方法
    private final Map<Class<?>, Invoker> invokers = new HashMap<>();

    // 标记注解，用来标记不同的处理函数
    @Target({ ElementType.METHOD })
    @Retention(RetentionPolicy.RUNTIME)
    public @interface Handler {
    }

    public class RedundantHandlerException extends RuntimeException {
        private final Class<?> messageClass;
        private final Method originalMethod;
        private final Method redundantMethod;

        public RedundantHandlerException(Class<?> messageClass, Method originalMethod,
                Method redundantMethod) {
            this.messageClass = messageClass;
            this.originalMethod = originalMethod;
            this.redundantMethod = redundantMethod;
        }

        @Override
        public String toString() {
            return "Duplicate handler method for " + messageClass + ", orginal method is "
                    + originalMethod + ", redundant method is " + redundantMethod;
        }
    }

    protected MessageCracker() {
        initialize(this);
    }

    public MessageCracker(Object messageHandler) {
        initialize(messageHandler);
    }

    // 初始化，扫描消息类型和处理函数，并注册在 invokers 中
    public void initialize(Object messageHandler) {
        // 扫描自身
        Class<?> handlerClass = messageHandler.getClass();
        for (Method method : handlerClass.getMethods()) {
            // 是否满足 Handler 方法的条件
            if (isHandlerMethod(method)) {
                Class<?> messageClass = method.getParameterTypes()[0];
                method.setAccessible(true);
                Invoker invoker = new Invoker(messageHandler, method);
                Invoker existingInvoker = invokers.get(messageClass);
                if (existingInvoker != null) {
                    throw new RedundantHandlerException(messageClass, existingInvoker.getMethod(),
                            method);
                }
                // 注册处理函数
                // 如：Logout -> onLogout
                //    Logon -> onLogon
                invokers.put(messageClass, invoker);
            }
        }
    }

    private boolean isHandlerMethod(Method method) {
        int modifiers = method.getModifiers();
        Class<?>[] parameterTypes = method.getParameterTypes();
        // 非私有方法，且 (是onMessage方法或者被 @Handler 注解), 且有两个参数，
        // 且第一个参数继承自Message类，且第二个参数是SessionID
        return !Modifier.isPrivate(modifiers) && matchesConventionOrAnnotation(method)
                && parameterTypes.length == 2 && Message.class.isAssignableFrom(parameterTypes[0])
                && parameterTypes[1] == SessionID.class;
    }

    private boolean matchesConventionOrAnnotation(Method method) {
        // 方法名称是 onMessage 或者被 @Handler 注解了
        return method.getName().equals("onMessage") || method.isAnnotationPresent(Handler.class);
    }

    private class Invoker {
        private final Object target;
        private final Method method;

        public Invoker(Object target, Method method) {
            this.target = target;
            this.method = method;
        }

        public Method getMethod() {
            return method;
        }

        public void Invoke(Message message, SessionID sessionID) throws IllegalArgumentException,
                IllegalAccessException, InvocationTargetException {
            method.invoke(target, message, sessionID);
        }
    }

    /**
     * Process ("crack") a FIX message and call the registered handlers for that type, if any
     */
    public void crack(quickfix.Message message, SessionID sessionID) throws UnsupportedMessageType,
            FieldNotFound, IncorrectTagValue {
        Invoker invoker = invokers.get(message.getClass());
        if (invoker != null) {
            try {
                // 执行方法调用
                invoker.Invoke(message, sessionID);
            } catch (InvocationTargetException ite) {
                try {
                    throw ite.getTargetException();
                } catch (UnsupportedMessageType | IncorrectTagValue | FieldNotFound e) {
                    throw e;
                } catch (Throwable t) {
                    propagate(t);
                }
            } catch (Exception e) {
                propagate(e);
            }
        } else {
            // 默认使用 onMessage 处理
            onMessage(message, sessionID);
        }
    }

    private void propagate(Throwable e) {
        if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
        } else if (e instanceof Error) {
            throw (Error) e;
        } else {
            throw new RuntimeException(e);
        }
    }

    /**
     * Fallback method that is called if no invokers are found.
     */
    protected void onMessage(quickfix.Message message, SessionID sessionID) throws FieldNotFound,
            UnsupportedMessageType, IncorrectTagValue {
        throw new UnsupportedMessageType();
    }
}
