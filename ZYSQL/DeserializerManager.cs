﻿
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq.Expressions;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;

namespace ZYSQL
{
    public class DeserializerManager
    {
        private static readonly DeserializerManager Instance = new DeserializerManager();

        private DeserializerManager()
        {
        }


        public ConcurrentDictionary<Type, Delegate> FuncDiy { get; set; } = new ConcurrentDictionary<Type, Delegate>();

        public static DeserializerManager GetInstance()
        {
            return Instance;
        }


        public Func<IDataReader, List<T>> GetFuncForType<T>(IDataReader read) where T : new()
        {
            var t = typeof(T);

            if (FuncDiy.ContainsKey(t))
                return (Func<IDataReader, List<T>>) FuncDiy[t];
            var func = GetTypeDeserializerImpl<T>(read);

            FuncDiy.AddOrUpdate(t, func, (a, b) => func);

            return func;
        }


        public static Func<IDataReader, List<T>> GetTypeDeserializerImpl<T>(IDataReader read) where T : new()
        {
            var type = typeof(T);
            var returnType = typeof(List<T>);
            var readType = typeof(IDataReader);
            var dr = typeof(IDataRecord);


            var dm = new DynamicMethod("Deserialize" + Guid.NewGuid(), returnType,
                new[] {typeof(IDataReader)}, type, true);
            var il = dm.GetILGenerator();

            var endref = il.DefineLabel();
            var next = il.DefineLabel();
            il.DefineLabel();


            var props = (from a in type.GetProperties(BindingFlags.Instance | BindingFlags.Public)
                         where a.CanWrite &&a.GetCustomAttribute<Ignore>()==null
                         select a).ToArray();


            var proplable = new Label[props.Length];

            for (var i = 0; i < props.Length; i++)
                proplable[i] = il.DefineLabel();


            il.DeclareLocal(returnType); //list<t> return
            il.DeclareLocal(type);
            il.DeclareLocal(typeof(object)); //read?
            il.DeclareLocal(typeof(int));
            il.DeclareLocal(typeof(int));
            il.DeclareLocal(typeof(string));

            il.Emit(OpCodes.Nop);
            il.Emit(OpCodes.Newobj, returnType.GetConstructor(new Type[] {}));
            il.Emit(OpCodes.Stloc_0);

            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Callvirt, dr.GetProperty(nameof(read.FieldCount)).GetMethod);
            il.Emit(OpCodes.Stloc_S, 4);
            il.Emit(OpCodes.Ldloc_S, 4);
            il.Emit(OpCodes.Brfalse, endref);


            ////while (read.Read())
            ////{
            ////    ItemBase tmp = new ItemBase();
            il.MarkLabel(next);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Callvirt, readType.GetMethod(nameof(read.Read)));
            il.Emit(OpCodes.Brfalse, endref);
            il.Emit(OpCodes.Newobj, type.GetConstructor(new Type[] {}));
            il.Emit(OpCodes.Stloc_1);


            il.Emit(OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Stloc_3);

            var fori = il.DefineLabel();
            il.MarkLabel(fori);

            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldloc_3);
            il.Emit(OpCodes.Callvirt, dr.GetMethod(nameof(read.GetName)));
            il.Emit(OpCodes.Stloc_S, 5);

            var ii = 0;

            var makeNext = il.DefineLabel();

            foreach (var item in props)
            {
                il.Emit(OpCodes.Ldloc_S, 5);
                il.Emit(OpCodes.Ldstr, item.Name);
                il.Emit(OpCodes.Call, typeof(string).GetMethod("op_Equality"));
                il.Emit(OpCodes.Brfalse_S, proplable[ii]);
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldloc_3);
                il.Emit(OpCodes.Callvirt, dr.GetMethod("get_Item", new[] {typeof(int)}));
                il.Emit(OpCodes.Stloc_2);
                il.Emit(OpCodes.Ldloc_2);
                il.Emit(OpCodes.Isinst, typeof(DBNull));
                il.Emit(OpCodes.Ldnull);
                il.Emit(OpCodes.Cgt_Un);
                il.Emit(OpCodes.Brtrue_S, proplable[ii]);
                il.Emit(OpCodes.Ldloc_2);
                il.Emit(OpCodes.Brfalse_S, proplable[ii]);

                if (item.PropertyType == typeof(string))
                {
                    il.Emit(OpCodes.Ldloc_1);
                    il.Emit(OpCodes.Ldloc_2);
                    il.Emit(OpCodes.Castclass, typeof(string));
                    il.Emit(OpCodes.Callvirt, item.SetMethod);
                }
                else
                {
                    il.Emit(OpCodes.Ldloc_1);
                    il.Emit(OpCodes.Ldloc_2);
                    FlexibleConvertBoxedFromHeadOfStack(il, typeof(object), item.PropertyType, null);
                    il.Emit(OpCodes.Callvirt, item.SetMethod);
                }

                il.Emit(OpCodes.Br, makeNext);


                il.MarkLabel(proplable[ii]);
                ii++;
            }

            il.MarkLabel(makeNext);
            il.Emit(OpCodes.Ldloc_3);
            il.Emit(OpCodes.Ldc_I4_1);
            il.Emit(OpCodes.Add);
            il.Emit(OpCodes.Stloc_3);
            il.Emit(OpCodes.Ldloc_3);
            il.Emit(OpCodes.Ldloc_S, 4);
            il.Emit(OpCodes.Clt);
            il.Emit(OpCodes.Brtrue, fori);


            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Ldloc_1);
            il.Emit(OpCodes.Callvirt, returnType.GetMethod(nameof(List<T>.Add)));
            il.Emit(OpCodes.Nop);
            il.Emit(OpCodes.Br, next);
            il.MarkLabel(endref);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Callvirt, readType.GetMethod(nameof(read.Close)));
            il.Emit(OpCodes.Ldloc_0);
            il.Emit(OpCodes.Ret);

            var funcType = Expression.GetFuncType(typeof(IDataReader), returnType);
            return (Func<IDataReader, List<T>>) dm.CreateDelegate(funcType);
        }

        private static void FlexibleConvertBoxedFromHeadOfStack(ILGenerator il, Type from, Type to, Type via)
        {
            MethodInfo op;
            if (from == (via ?? to))
            {
                il.Emit(OpCodes.Unbox_Any, to); // stack is now [target][target][typed-value]
            }
            else if ((op = GetOperator(from, to)) != null)
            {
                // this is handy for things like decimal <===> double
                il.Emit(OpCodes.Unbox_Any, from); // stack is now [target][target][data-typed-value]
                il.Emit(OpCodes.Call, op); // stack is now [target][target][typed-value]
            }
            else
            {
                var handled = false;
                var opCode = default(OpCode);
                switch (TypeExtensions.GetTypeCode(from))
                {
                    case TypeCode.Boolean:
                    case TypeCode.Byte:
                    case TypeCode.SByte:
                    case TypeCode.Int16:
                    case TypeCode.UInt16:
                    case TypeCode.Int32:
                    case TypeCode.UInt32:
                    case TypeCode.Int64:
                    case TypeCode.UInt64:
                    case TypeCode.Single:
                    case TypeCode.Double:
                        handled = true;
                        switch (TypeExtensions.GetTypeCode(via ?? to))
                        {
                            case TypeCode.Byte:
                                opCode = OpCodes.Conv_Ovf_I1_Un;
                                break;
                            case TypeCode.SByte:
                                opCode = OpCodes.Conv_Ovf_I1;
                                break;
                            case TypeCode.UInt16:
                                opCode = OpCodes.Conv_Ovf_I2_Un;
                                break;
                            case TypeCode.Int16:
                                opCode = OpCodes.Conv_Ovf_I2;
                                break;
                            case TypeCode.UInt32:
                                opCode = OpCodes.Conv_Ovf_I4_Un;
                                break;
                            case TypeCode.Boolean: // boolean is basically an int, at least at this level
                            case TypeCode.Int32:
                                opCode = OpCodes.Conv_Ovf_I4;
                                break;
                            case TypeCode.UInt64:
                                opCode = OpCodes.Conv_Ovf_I8_Un;
                                break;
                            case TypeCode.Int64:
                                opCode = OpCodes.Conv_Ovf_I8;
                                break;
                            case TypeCode.Single:
                                opCode = OpCodes.Conv_R4;
                                break;
                            case TypeCode.Double:
                                opCode = OpCodes.Conv_R8;
                                break;
                            default:
                                handled = false;
                                break;
                        }
                        break;
                }
                if (handled)
                {
                    il.Emit(OpCodes.Unbox_Any, from); // stack is now [target][target][col-typed-value]
                    il.Emit(opCode); // stack is now [target][target][typed-value]
                    if (to == typeof(bool))
                    {
                        // compare to zero; I checked "csc" - this is the trick it uses; nice
                        il.Emit(OpCodes.Ldc_I4_0);
                        il.Emit(OpCodes.Ceq);
                        il.Emit(OpCodes.Ldc_I4_0);
                        il.Emit(OpCodes.Ceq);
                    }
                }
                else
                {
                    il.Emit(OpCodes.Ldtoken, via ?? to); // stack is now [target][target][value][member-type-token]
                    il.EmitCall(OpCodes.Call, typeof(Type).GetMethod(nameof(Type.GetTypeFromHandle)), null);
                        // stack is now [target][target][value][member-type]
                    il.EmitCall(OpCodes.Call,
                        typeof(Convert).GetMethod(nameof(Convert.ChangeType), new[] {typeof(object), typeof(Type)}),
                        null); // stack is now [target][target][boxed-member-type-value]
                    il.Emit(OpCodes.Unbox_Any, to); // stack is now [target][target][typed-value]
                }
            }
        }


        private static MethodInfo GetOperator(Type from, Type to)
        {
            if (to == null) return null;
            MethodInfo[] fromMethods, toMethods;
            return ResolveOperator(fromMethods = from.GetMethods(BindingFlags.Static | BindingFlags.Public), from, to,
                       "op_Implicit")
                   ??
                   ResolveOperator(toMethods = to.GetMethods(BindingFlags.Static | BindingFlags.Public), from, to,
                       "op_Implicit")
                   ?? ResolveOperator(fromMethods, from, to, "op_Explicit")
                   ?? ResolveOperator(toMethods, from, to, "op_Explicit");
        }

        private static MethodInfo ResolveOperator(MethodInfo[] methods, Type from, Type to, string name)
        {
            foreach (MethodInfo t in methods)
            {
                if (t.Name != name || t.ReturnType != to) continue;
                var args = t.GetParameters();
                if (args.Length != 1 || args[0].ParameterType != from) continue;
                return t;
            }
            return null;
        }
    }


    internal static class TypeExtensions
    {
        public static string Name(this Type type)
        {
            return type.Name;
        }

        public static bool IsValueType(this Type type)
        {
            return type.IsValueType;
        }

        public static bool IsEnum(this Type type)
        {
            return type.IsEnum;
        }

        public static bool IsGenericType(this Type type)
        {
            return type.IsGenericType;

        }

        public static bool IsInterface(this Type type)
        {

            return type.IsInterface;

        }

        public static TypeCode GetTypeCode(Type type)
        {
            return Type.GetTypeCode(type);
        }


        public static MethodInfo GetPublicInstanceMethod(this Type type, string name, Type[] types)
        {           
            return type.GetMethod(name, BindingFlags.Instance | BindingFlags.Public, null, types, null);
        }
    }
}

