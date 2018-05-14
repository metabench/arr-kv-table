var tools = require('lang-mini');
var each = tools.each;

var tof = tools.tof;

var is_arr_of_strs = tools.is_arr_of_strs;

var are_equal = require('deep-equal');;
var clone = require('clone');

var set_arr_tree_value = tools.set_arr_tree_value;
var get_arr_tree_value = tools.get_arr_tree_value;
var deep_arr_iterate = tools.deep_arr_iterate;

// lang-tools
// lang-mini
//  would be fine for Evented_Class too.



//  like lang essentials. Would maybe have a bit more. Maybe even Evented_Class?
//  does not contain Evented_Class or Data_Objects
//   lang-core could have them.






// Each item in the array will have key and values
//  Could have multiple items in the key
//   The key contains some of its values possibly

// arr_values is arr_kvs
// then its fields?
//  

// call it keys or fields.
//  the keys array has got split into both keys and values
//   keys for the keys, keys for the values

var Number_Type_Info = require('number-type-info');

// is_arr_kvp

class Array_KV_Table {
    constructor(arr_kv_field_names, arr_records) {
        var a = arguments;
        a.l = a.length;


        if (a.l === 1) {
            this.keys = Object.keys(a[0][0]);
            this.values = this.records = a[0].map(item => {
                let item_res = [];
                each(item, (v, i) => {
                    item_res.push(v);
                })
                return item_res;
            });
        }

        if (a.l === 2) {
            this.keys = this.fields = a[0];


            if (Array.isArray(a[1])) {
                this.values = this.records = a[1];
            } else {
                if (typeof a[1] === 'number') {
                    // we just know how many records to allocate space for. But we don't need to allocate anyway.

                    // it's just a count, don't need it here.


                }
            }

        }
        this.create_map_fields();
    }

    // select function.
    //  will get a new AKVT with only those fields selected.

    select(fields) {
        let field_ids = [];
        //let field_names = [];



        let collect = (obj_or_arr, fn) => {
            let res = [];
            each(obj_or_arr, v => res.push(fn(v)));
            return res;
        }

        each(fields, field => {
            if (typeof field === 'number') {
                field_ids.push(field);
                //field_names.push()
            } else if (typeof field === 'string') {
                field_ids.push(this.map_keys[field]);
                //field_names.push(field);
            }
        });



        // Seems like the collect function works well for creating an array out of results.

        // Not sure it's that different to .map


        // no the map function does the trick, need to apply it to the right objects.


        let field_names = field_ids.map(field_id => this.map_keys_by_id[field_id]);


        let arr_res = this.values.map(value => field_ids.map(field_id => value[field_id]));




        //let arr_res = collect(this.values, value => collect(field_ids, field_id => value[field_id]));


        //each(this.values, value => arr_res.push(collect(field_ids, field_id => value[field_id])));

        //let arr_res = [];

        /*
        each(this.values, value => {


            let arr_item = [];
            each(field_ids, field_id => {
                arr_item.push(value[field_id]);
            })
            arr_res.push(arr_item);

            //return arr_item;
        })
        */
        //console.log('arr_res', arr_res);
        //console.log('field_names', field_names);
        return new Array_KV_Table(field_names, arr_res);


    }

    create_map_fields() {


        //console.log('this.keys', this.keys);
        //throw 'stop';

        var map_keys = this.map_fields = this.map_keys = {};
        // 
        let map_keys_by_id = this.map_keys_by_id = {};
        each(this.keys, (v, i) => {
            map_keys[v] = i;
            map_keys_by_id[i] = v;
        });
        /*
        each(this.keys[1], (v, i) => {
            map_keys[v] = [1, i];
            map_keys_by_id[i] = v;
        });
        */
        return map_keys;
    }

    get length() {
        return this.values.length;
    }

    flatten(arr_field_names) {
        // need to have a field map as well.

        // need to refer to the field map

        var arr_indexes = [];
        var map_fields = this.map_fields;

        each(arr_field_names, (field_name) => {
            var idx = map_fields[field_name];
            if (idx) {
                arr_indexes.push(idx);
            }

        });
        //console.log('arr_indexes', arr_indexes);

        var res = [];

        //console.log('this.records[0]', this.records[0]);

        each(this.records, (record) => {
            var res_row = [];
            each(arr_indexes, (idx) => {

                /*
                if (idx[0] === 0) {
                    //res_row.push(record[0][idx[1]]);
                }
                if (idx[0] === 1) {
                    //res_row.push(record[1][idx[1]]);
                }
                */
                res_row.push(record[idx[0]][idx[1]]);

            });
            res.push(res_row);

        })
        //throw 'stop';
        return res;

    }

    // Basic scan

    // Could make a deeper scan that looks into arrays that it finds.

    // Go into the arrays and compare / record the types
    // Go into more detail on the number types. Find out the max, min, max precision needed.

    // Should be able to vary the Satoshi encoding used.
    //  sat8, sat16, sat32
    //  flexible satoshi encoding.
    //  seems like another option to xas2.
    //  maybe have the different satoshi encodings as individual options in Binary_Encoding, as well as flexible satoshi.
    //   would be like xas2 but for satoshi values.
    //    many snapshot records in the db could be reencoded with these
    //     may take a lower level operation to reencode a table. May be worth making a reencoded version and then swapping them out.

    // The flexibility and extensibility in the encoding system will allow for relatively easy re-encoding of records, and changing to more specific or compressed data types.
    //  This would make a lot of sense with distributing well compressed data to different peers on a network, while they possibly run different versions.
    //   Could maintain records of what is the most recent encoding structure. Want to have smooth upgrades.






    // This may be useful for a basic scan.
    //  Can see where they are numbers, and not mixed.
    //  May be worth doing an advanced scan for numbers too.

    // can do a scan on this.keys


    basic_scan_field_types() {
        // Do a number types info scan.
        //  or basic scan

        // Types info?

        var key_keys = new Array(this.keys[0].length);
        var value_keys = new Array(this.keys[1].length);

        var process_arr = (arr) => {
            var t;
            var res = [];
            // 
            each(arr, (item) => {
                t = tof(item);
                if (t === 'array') {
                    res.push(process_arr(item));
                } else {
                    res.push(t);
                }
            })
            return res;
        }

        var process_key_or_value = (key_arr, k_or_v) => {
            each(k_or_v, (key_item, i) => {
                var t = tof(key_item),
                    t_existing;
                if (t === 'array') {
                    if (!key_arr[i]) {
                        key_arr[i] = process_arr(key_item);
                    } else {
                        var a = process_arr(key_item);

                        // Deeper check - does not check if they are equal.
                        //  Goes into the data structure to process specific numbers to find their ranges.

                        // Can do it for the non-array sources first.

                        if (are_equal(key_arr[i], a)) {

                        } else {
                            key_arr[i] = 'mixed';
                        }
                    }
                } else {
                    //console.log('key_keys[i]', key_keys[i]);
                    if (!key_arr[i]) {
                        key_arr[i] = t;
                    } else {
                        if (key_arr[i] === t) {

                        } else {
                            key_arr[i] = 'mixed';
                        }
                    }
                }
            });
        }

        each(this.records, (record) => {
            var record_key = record[0],
                record_val = record[1];
            process_key_or_value(key_keys, record_key);
            process_key_or_value(value_keys, record_val);
            //process_key_or_value(this.keys, record);

        });

        var res = [key_keys, value_keys];
        return res;

    }

    advanced_scan_field_types() {
        var basic = this.basic_scan_field_types();
        //console.log('basic', basic);
        var key_keys = new Array(this.keys[0].length);
        var value_keys = new Array(this.keys[1].length);

        // recursively? go through it testing the numbers

        // recursively traverse arrays, get indexes to use in other array structure.
        //  Need to refer to each record recursively, refer it to the field data in the structure.

        // keep recursively processing the basic scan.

        var scan = clone(basic);

        // A sequence to reach that spot?
        //  put together an array of the indexes.
        //   
        // Function to iterate an array structure.
        //  deep iterate.

        // Should have a path array.
        //  Pass it through



        //var c = 0;

        deep_arr_iterate(scan, [], (path, item) => {
            //console.log('\n');
            //console.log('path', path);
            //console.log('item', item);

            // then changing the array items in place, or in a new structure.
            // could rebuild the structure...
            //set_arr_tree_value(scan, path, c++);

            if (item === 'number') {
                // replace it with one of these number range thingies.

                var nti = new Number_Type_Info();
                set_arr_tree_value(scan, path, nti);

            }
        });

        // then do this array iterate through each of the records, setting the path within 

        //console.log('scan', scan);

        each(this.records, (record) => {
            var record_key = record[0],
                record_val = record[1];

            // then do the iterate through the records, processing numbers with that very path.
            // then go through the scan for each record.

            deep_arr_iterate(scan, [], (path, item) => {
                //console.log('\n');
                //console.log('path', path);
                //console.log('item', item);

                if (item instanceof Number_Type_Info) {
                    // find the number in the record path

                    var item_in_record = get_arr_tree_value(record, path);
                    //console.log('item_in_record', item_in_record);

                    item.process(item_in_record);

                }

                // then changing the array items in place, or in a new structure.

                // could rebuild the structure...


                //set_arr_tree_value(scan, path, c++);

                /*
    
                if (item === 'number') {
                    // replace it with one of these number range thingies.
    
                    //var nti = new Number_Type_Info();
                    //set_arr_tree_value(scan, path, nti);

                    var nti = get_arr_tree_value(scan, path);
                    //console.log('nti', nti);

                    var record_val = get_arr_tree_value(record, path);
                    console.log('record_val', record_val);



                    //nti.process()






    
    
                }
                */
            });

            //var nti = get_arr_tree_value(record);

        });

        //console.log('scan', scan);

        return scan;



        //throw 'stop';






        /*

        var detailed_number_scan = (scan, index = 0, depth = 0) => {
            each(scan, (item, i) => {
                var t = tof(item);

                if (t === 'array') {
                    detailed_number_scan(item, i, depth + 1)
                } else if (t === 'number') {
                    
                } else {
                    
                }
            })

        }
        detailed_number_scan(scan);

        */

        /*


        var process_arr = (arr) => {
            var t;
            var res = [];
            // 
            each(arr, (item) => {
                t = tof(item);
                if (t === 'array') {
                    res.push(process_arr(item));
                } else {
                    res.push(t);
                }
            })
            return res;
        }

        var process_key_or_value = (key_arr, k_or_v) => {
            each(k_or_v, (key_item, i) => {
                var t = tof(key_item), t_existing;
                if (t === 'array') {
                    if (!key_arr[i]) {
                        //key_arr[i] = process_arr(key_item);
                    } else {
                        //var a = process_arr(key_item);
                        
                        // Deeper check - does not check if they are equal.
                        //  Goes into the data structure to process specific numbers to find their ranges.

                        // Can do it for the non-array sources first.
                        
                        //if (are_equal(key_arr[i], a)) {

                        //} else {
                        //    key_arr[i] = 'mixed';
                        //}
                    }
                } else {
                        //console.log('key_keys[i]', key_keys[i]);
                    if (!key_arr[i]) {
                        key_arr[i] = t;
                    } else {
                        if (key_arr[i] === t) {

                        } else {
                            key_arr[i] = 'mixed';
                        }
                    }
                }
            });
        }

        each(this.records, (record) => {
            var record_key = record[0], record_val = record[1];
            process_key_or_value(key_keys, record_key);
            process_key_or_value(value_keys, record_val);
        });

        var res = [key_keys, value_keys];

        */

        // for every field, needs to scan various types.



        // Needs a similar recursive system to before, but with the 'number' properties will test their types.
        //  Then will use this info to set up more precisely typed table storage for the data in memory.

        // Do the scan again, referring to the basic one, but where there are numbers, process the numbers with the number info.
        //  This will get detailed info about what number types are used throughout.
        //   Then can choose appropriate data types to represent them as in Typed Arrays.
        //    These typed array objects will be dynamically created when loaded from the DB, so if the size of numbers were to grow, the data analysis object
        //     would have some different types.

        // This more efficient encoding will help with storage and transmission of db records.
        //  Definitely looks like Binary_Encoding is going to expand to deal with more numeric types.

        // Going to soon have very efficient typed array tables of data that gets downloaded from the DB.
        //  A slightly later challenge would be to transmit the data in this form.

        // Now that the data collector is going, it seems best to work on replication from that.
        //  Copying all of the data from it would make a lot of sense.
        //   Making a new version that is able to import the whole db would be a lot of use.
        //    Also, automatic indexing of records would make a lot of sense.

        // Want it to handle larger amounts of data in a streaming fashion.

        // then clone the basic scan, do another scan, and replace 'number' strings in the basic scan with the Number_Type_Info object.




        //  Also 







        //throw 'stop';
    }




    scan_field_types() {

        //var ta_Int8 = new Int8Array(1);
        //var ta_Uint8 = new Uint8Array(1);

        // go through all of the records.

        // this.keys

        // the found data for each of the fields in the records.
        //  Check if they are all numeric first.

        // Then look within the numbers to see the max size / precision.

        var key_keys = new Array(this.keys[0].length);
        var value_keys = new Array(this.keys[1].length);



        each(this.records, (record) => {
            var record_key = record[0],
                record_val = record[1];

            // Will need to look at sizes.
            //  Could also satoshise some numbers.
            //   (ie * 100m, turn to integers)

            // Possiblity of getting data from the server as satoshis.

            // Converting to satoshis seems like one of those specific issues / problems right now.
            //  Makes sense for an integer internal working number system.

            // However, worth working with the fractional number system for the moment.
            //  More aptitude in dealing with that will help conversion.

            // Though it looks like we would need float64 for small numbers of satoshis to get the right precision.
            //  We could change types to a fractional type, using a fraction as a unit, and then the number could be stored as a small integer.

            // Getting the data into a client node's memory for rapid use is a major step, one of the main milestones.

            // Will then need to do streaming / incremental downloads of the data when it gets much larger after more time.
            //  Need to get this working right for the ability to handle much larger amounts of data.

            // Probably worth setting up security before sharding.
            // Systemic distribution by table sounds useful.
            //  Functionality for storing, checking and changing the distribution system.
            //   Easiest to copy data over to other servers.
            //    Possibility to move a table to a (new) shard?
            //     Keep the most frequently accessed records
            //      Measure how frequently / when each record is accessed - this takes more writes.





            //  Definitely want to get sharding set up correctly.
            //   Would make use of some low level functionality, as well as having the server use client connections to other servers.













            // get the type for all of them.

            // have arrays with the keys' and values' types.

            // numbers need to be looked at relative to the max.
            //  find the max number of decimal points.

            // Need to work out how accurate the various number types are.
            //  Could try out typed arrays to see if the items fit in the value.



            // try assigning them?

            // Float32Array
            //  7 significant digits e.g. 1.1234567

            // Float64Array
            //  16 significant digits e.g. 1.123...15

            // Encoding satoshi data seems like it would be very useful.
            //  Don't want a flippening bug.

            // UInt32
            //  0 to 4294967295

            // 1 btc = 100000000 sat.
            //  So can multiply a whole bunch of numbers by 100000000
            //   Then easily can fit in a 32 bit number, easily encodable into an xas2 as well.

            // Uint16Array	0 to 65535
            //  So a fair few coins could be encoded as 16 bit.

            // Could fairly easily store number of satoshis in the db.
            //  Up to 42.94967295 times the value of bitcoin. A while off if it ever happens.

            // Need to do more work on satoshi numeric types.
            //  May start to encode them into the db instead...
            //  Could expand the encodings types to enable this.
            //  A new Satoshi xas2 data type would be useful.

            // Possible function to convert data to satoshi integer data, in place.

            // For the moment, keep the db stored in the same way as it was.
            //  Make an option to store satoshis as integers.

            // For the moment, load the data into arr-kv-table or the typed version.

            var process_key_arr = (arr) => {
                var t;
                var res = [];
                // 
                each(arr, (item) => {
                    t = tof(item);
                    if (t === 'array') {
                        res.push(process_key_arr(item));
                    } else {
                        res.push(t);
                    }
                })
                return res;
            }

            each(record_key, (key_item, i) => {
                //console.log('key_item', key_item);

                // look at the type
                //  if it's still the same type as before, then we can use that type.

                var t = tof(key_item),
                    t_existing;

                // if its an array, then do it in a nested way...?
                //  multi level nesting?

                // could ignore it for the moment?
                //  would take creation of multi-level arrays
                //   still need to encode these arrays (nested if necessary) into typed arrays.
                //    Definitely seems tricky, could need to go into more detail on field definitions.

                // Want to check if the arrays are all the same in terms of data type.
                //  Will scan numbers to see if they would fit a sat32 or even sat16 data type. (Could even try Sat24)

                // Getting this data from the server to use as efficient typed array structures that automatically update will be a major step.
                //  Then it will be possible to do many readings and comparisons quickly on these typed array structures.
                // When in RAM, simply / quickly organised into typed arrays, it will be available very quickly.
                //  All sorts of things could be tested and alerts found.












                // mainly want to 

                if (t === 'array') {
                    if (!key_keys[i]) {
                        key_keys[i] = process_key_arr(key_item);
                    } else {
                        var a = process_key_arr(key_item);


                        // is it different?
                        //t_existing = key_keys[i]

                        //console.log('key_keys[i] === t', key_keys[i] === t);
                        if (are_equal(key_keys[i], a)) {

                        } else {
                            key_keys[i] = 'mixed';
                        }

                        /*
                        if (key_keys[i] === t) {

                        } else {
                            key_keys[i] = 'mixed';
                        }
                        */

                    }

                } else {
                    //console.log('key_keys[i]', key_keys[i]);
                    if (!key_keys[i]) {
                        key_keys[i] = t;
                    } else {
                        // is it different?
                        //t_existing = key_keys[i]

                        //console.log('key_keys[i] === t', key_keys[i] === t);

                        if (key_keys[i] === t) {

                        } else {
                            key_keys[i] = 'mixed';
                        }

                    }
                }






            });

            var process_value_arr = (arr) => {
                var t;
                var res = [];
                // 
                each(arr, (item) => {
                    t = tof(item);
                    if (t === 'array') {
                        res.push(process_value_arr(item));
                    } else {
                        res.push(t);
                    }
                })
                return res;
            }

            each(record_val, (val_item, i) => {
                //console.log('val_item', val_item);

                var t = tof(val_item),
                    t_existing;


                if (t === 'array') {
                    if (!value_keys[i]) {
                        value_keys[i] = process_value_arr(val_item);

                        //var a = process_value_arr(val_item);
                        //console.log('a', a);

                        // recursively? go through the val_item



                    } else {

                        var a = process_value_arr(val_item);
                        //console.log('a', a);

                        // is it different?
                        //t_existing = key_keys[i]

                        //console.log('key_keys[i] === t', key_keys[i] === t);


                        if (are_equal(value_keys[i], a)) {

                        } else {
                            value_keys[i] = 'mixed';
                        }


                    }

                } else {
                    //console.log('key_keys[i]', key_keys[i]);
                    if (!value_keys[i]) {
                        value_keys[i] = t;
                    } else {
                        // is it different?
                        //t_existing = key_keys[i]

                        //console.log('key_keys[i] === t', key_keys[i] === t);

                        if (value_keys[i] === t) {

                        } else {
                            value_keys[i] = 'mixed';
                        }

                    }
                }

                //console.log('key_keys[i]', key_keys[i]);

            });

        });

        var res = [key_keys, value_keys];
        return res;

    }




}

// Arrkv static processing methods...?


module.exports = Array_KV_Table;