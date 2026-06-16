<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        Schema::table('Fact_Course_Delivery', function (Blueprint $table) {
            // Dynamic Age Range Count Columns
            $table->integer('Count_Age_Range_18_24')->default(0)->after('Count_Ethnicity_Not_Stated');
            $table->integer('Count_Age_Range_25_34')->default(0)->after('Count_Age_Range_18_24');
            $table->integer('Count_Age_Range_35_44')->default(0)->after('Count_Age_Range_25_34');
            $table->integer('Count_Age_Range_45_54')->default(0)->after('Count_Age_Range_35_44');
            $table->integer('Count_Age_Range_55_64')->default(0)->after('Count_Age_Range_45_54');
            $table->integer('Count_Age_Range_Over_65')->default(0)->after('Count_Age_Range_55_64');
            $table->integer('Count_Age_Range_Not_Stated')->default(0)->after('Count_Age_Range_Over_65');

            // Ethnicity, Privacy, Assets & Repeat parameters
            $table->integer('Count_Ethnicity_White_Traveller')->default(0)->after('Count_Ethnicity_White_Irish');
            $table->integer('Count_SEND_Not_Stated')->default(0)->after('Count_SEND');
            $table->integer('Count_Pupil_Premium_Not_Stated')->default(0)->after('Count_Pupil_Premium');
            $table->integer('Count_Bikes_Swapped')->default(0)->after('Count_Age_Range_Over_65');
            $table->integer('Count_Bikes_Recycled')->default(0)->after('Count_Bikes_Swapped');
            $table->string('Count_Booked_Repeat_Type_Na', 20)->nullable()->after('Count_Bikes_Recycled');
            $table->string('Count_Booked_Repeat_Type_Unique', 20)->nullable()->after('Count_Booked_Repeat_Type_Na');
            $table->string('Count_Booked_Repeat_Type_Repeat', 20)->nullable()->after('Count_Booked_Repeat_Type_Unique');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::table('Fact_Course_Delivery', function (Blueprint $table) {
            $table->dropColumn(['Count_Age_Range_18_24','Count_Age_Range_25_34','Count_Age_Range_35_44',
                'Count_Age_Range_45_54','Count_Age_Range_55_64','Count_Age_Range_Over_65','Count_Age_Range_Not_Stated',
                'Count_Ethnicity_White_Traveller','Count_SEND_Not_Stated','Count_Pupil_Premium_Not_Stated','Count_Bikes_Swapped',
                'Count_Bikes_Recycled','Count_Booked_Repeat_Type_Na','Count_Booked_Repeat_Type_Unique','Count_Booked_Repeat_Type_Repeat'
                ]);
        });
    }
};
