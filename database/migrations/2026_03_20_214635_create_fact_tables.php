<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        // Fact_Course_Delivery
        Schema::connection('mysql')
            ->create('Fact_Course_Delivery', function (Blueprint $table) {
                $table->id('Delivery_Fact_Key');

                // Note: Date_Key remains INT as it follows YYYYMMDD format from Dim_Date
                $table->integer('Date_Key')->nullable();

                // All other Dimension Keys must be unsignedBigInteger to match ->id()
                $table->unsignedBigInteger('Delivery_Key');
                $table->unsignedBigInteger('School_Key')->nullable();
                $table->unsignedBigInteger('Organisation_Key')->nullable();
                $table->unsignedBigInteger('Course_Key');
                $table->unsignedBigInteger('Provider_Key');
                $table->unsignedBigInteger('Grant_Key')->nullable();

                // Activity Metrics
                $table->integer('Riders_Enrolled_Count')->default(0);
                $table->integer('Riders_Completed_Count')->default(0);
                $table->decimal('Total_Cost', 10, 2)->default(0);

                // Demographic Metrics
                $table->integer('Count_Female')->default(0);
                $table->integer('Count_Male')->default(0);
                $table->integer('Count_Ethnicity_White')->default(0);
                $table->integer('Count_Ethnicity_Asian')->default(0);
                $table->integer('Count_Ethnicity_Black')->default(0);
                $table->integer('Count_Ethnicity_Mixed')->default(0);
                $table->integer('Count_Ethnicity_Other')->default(0);

                // Foreign Key Constraints
                $table->foreign('Date_Key')->references('Date_Key')->on('Dim_Date');
                $table->foreign('Delivery_Key')->references('Delivery_Key')->on('Dim_Delivery_Header');
                $table->foreign('School_Key')->references('School_Key')->on('Dim_School');
                $table->foreign('Course_Key')->references('Course_Key')->on('Dim_Course');
                $table->foreign('Provider_Key')->references('Provider_Key')->on('Dim_Training_Provider');
                $table->foreign('Grant_Key')->references('Grant_Key')->on('Dim_Grant');
            });

        // Fact_Parent_Survey
        Schema::connection('mysql')
            ->create('Fact_Parent_Survey', function (Blueprint $table) {
                $table->id('Parent_Survey_Key');
                $table->integer('Date_Key');
                $table->unsignedBigInteger('Rider_Key');
                $table->unsignedBigInteger('Course_Key');
                $table->unsignedBigInteger('Delivery_Key');
                $table->unsignedBigInteger('Grant_Key')->nullable();

                $table->integer('Rider_Emotion')->nullable();
                $table->integer('Pref_More_Training')->nullable();
                $table->integer('Pref_Interest_In_Training')->nullable();
                // Scores (Mapped to meaningful strings or integers)
                // values held are 1 to 6 (map to answers such as 1 - More Confident, 2 - a Little more confident, etc
                $table->integer('Confidence_Bike_General')->nullable();
                $table->integer('Confidence_Road')->nullable();
                $table->integer('Confidence_Independent')->nullable();
                $table->integer('Frequency_School')->nullable();
                $table->integer('Frequency_Leisure')->nullable();
                $table->integer('Frequency_Exercise')->nullable();

                // Feedback Flags (Pivoted from parent_survey_feedback)
                $table->boolean('Feedback_Is_Fun')->default(0);
                $table->boolean('Feedback_Is_Hard')->default(0);
                $table->boolean('Feedback_Is_Healthy')->default(0);
                $table->boolean('Feedback_Still_New')->default(0);
                $table->boolean('Feedback_Family_Friends')->default(0);
                $table->boolean('Feedback_Dont_See_Others_Like_Me')->default(0);
                $table->boolean('Feedback_On_Own')->default(0);
                $table->boolean('Feedback_Not_Enjoy')->default(0);
                $table->boolean('Feedback_None_Apply')->default(0);
                $table->string('Feedback_None_Apply_Input', 500)->nullable();

                $table->boolean('Encouragement_Use_Bike')->nullable()->default(0);
                $table->boolean('Encouragement_Use_Bike_On_Road')->nullable()->default(0);

                //Optional Questions
                // OQ1 Encouragers (Boolean Flags)
                $table->tinyInteger('Encourage_More_Direct_Routes')->nullable()->default(0); // oq1_o1
                $table->tinyInteger('Encourage_Local_Route_Awareness')->nullable()->default(0); // oq1_o2
                $table->tinyInteger('Encourage_Storage')->nullable()->default(0); // oq1_o3
                $table->tinyInteger('Encourage_Road_Surfaces')->nullable()->default(0); // oq1_o3
                $table->tinyInteger('Encourage_Confidence')->nullable()->default(0); // oq1_o5
                $table->tinyInteger('Encourage_Cycle_Maintenance')->nullable()->default(0); // oq1_o6
                $table->tinyInteger('Encourage_Local_Initiatives')->nullable()->default(0); // oq1_o7
                $table->tinyInteger('Encourage_Purchase_Ability')->nullable()->default(0); // oq1_o8
                $table->tinyInteger('Encourage_Doesnt_Want_To_Cycle_More')->nullable()->default(0); // oq1_o9
                $table->tinyInteger('Encourage_None')->nullable()->default(0); // oq1_null
                $table->string('Encourage_Other_Reason', 500)->nullable(); // optional_questions_input

                // OQ2-OQ19 Life Skills (Likert scale response as Pivoted Integers 1-5)
                $table->tinyInteger('Likert_Life_Skill')->nullable(); // oq2
                $table->tinyInteger('Likert_Self_Esteem')->nullable(); // oq3
                $table->tinyInteger('Likert_Fitness')->nullable(); // oq4
                $table->tinyInteger('Likert_Active')->nullable(); // oq5
                $table->tinyInteger('Likert_Mindfulness')->nullable(); // oq6
                $table->tinyInteger('Likert_Improve_Self_Regulate')->nullable(); // oq7
                $table->tinyInteger('Likert_Improve_Concentration')->nullable(); // oq8
                $table->tinyInteger('Likert_Improve_Academic_Performance')->nullable(); // oq9
                $table->tinyInteger('Likert_Independence')->nullable(); // oq10
                $table->tinyInteger('Likert_Improve_Road_Awareness')->nullable(); // oq11
                $table->tinyInteger('Likert_Improve_Environment_Awareness')->nullable(); // oq12
                $table->tinyInteger('Likert_Help_Socialise')->nullable(); // oq13
                $table->tinyInteger('Likert_Make_Children_Happy')->nullable(); // oq14
                $table->tinyInteger('Likert_Keep_Children_Occupied')->nullable(); // oq15
                $table->tinyInteger('Likert_Encourage_Children_Outside')->nullable(); // oq16
                $table->tinyInteger('Likert_Children_Less_Dependent')->nullable(); // oq17
                $table->tinyInteger('Likert_Reduce_Other_Transport_Expense')->nullable(); // oq18
                $table->tinyInteger('Likert_Enable_Cycle_As_Family')->nullable(); // oq19

                //Recommendation
                $table->integer('Likely_To_Recommend')->nullable()->default(0);

                // Metadata
                $table->unsignedBigInteger('Source_Survey_Id');
                $table->foreign('Date_Key')->references('Date_Key')->on('Dim_Date');
            });

        // Fact_HandsUp_Survey
        Schema::connection('mysql')
            ->create('Fact_HandsUp_Survey', function (Blueprint $table) {
                $table->id('Survey_Fact_Key');
                $table->integer('Date_Key')->nullable();
                $table->unsignedBigInteger('Delivery_Key');
                $table->unsignedBigInteger('Course_Key');

                // Question 1: Experience (Enjoyment) of training
                $table->integer('Exp_Enjoyed')->default(0);
                $table->integer('Exp_Did_Not_Enjoy')->default(0);
                $table->integer('Exp_Not_Sure')->default(0);
                $table->integer('Exp_Absent')->default(0);

                // Question 2: Baseline (Cycled before?)
                $table->integer('Base_Yes')->default(0);
                $table->integer('Base_No')->default(0);
                $table->integer('Base_Not_Sure')->default(0);

                // Question 3: Safety after training
                $table->integer('Safe_More')->default(0);
                $table->integer('Safe_Less')->default(0);
                $table->integer('Safe_No_Diff')->default(0);
                $table->integer('Safe_Not_Sure')->default(0);

                // Question 4: Confidence after training
                $table->integer('Conf_More')->default(0);
                $table->integer('Conf_Less')->default(0);
                $table->integer('Conf_No_Diff')->default(0);
                $table->integer('Conf_Not_Sure')->default(0);

                $table->foreign('Date_Key')->references('Date_Key')->on('Dim_Date');
                $table->foreign('Course_Key')->references('Course_Key')->on('Dim_Course');
            });
    }

    public function down(): void
    {
        Schema::dropIfExists('Fact_HandsUp_Survey');
        Schema::dropIfExists('Fact_Parent_Survey');
        Schema::dropIfExists('Fact_Course_Delivery');
    }
};
